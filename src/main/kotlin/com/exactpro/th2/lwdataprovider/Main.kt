/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.metrics.liveness
import com.exactpro.th2.common.metrics.readiness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.http.HttpServer
import io.ktor.server.engine.*
import io.ktor.util.*
import kotlinx.atomicfu.locks.ReentrantLock
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Condition
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

class Main {

    private val configurationFactory: CommonFactory

    private val context: Context

    private val resources: Deque<AutoCloseable> = ConcurrentLinkedDeque()
    private val lock = ReentrantLock()
    private val condition: Condition = lock.newCondition()

    @InternalAPI
    constructor(args: Array<String>) {

        configureShutdownHook(resources, lock, condition)

        configurationFactory = CommonFactory.createFromArguments(*args)
        resources += configurationFactory

        val configuration =
            Configuration(configurationFactory.getCustomConfiguration(CustomConfigurationClass::class.java))

        context = Context(
            configuration,

            cradleManager = configurationFactory.cradleManager.also {
                resources += AutoCloseable { it.dispose() }
            },
            messageRouterRawBatch = configurationFactory.messageRouterRawBatch.also {
                resources += it
            },
            messageRouterParsedBatch = configurationFactory.messageRouterParsedBatch.also {
                resources += it
            },
            grpcConfig = configurationFactory.grpcConfiguration
        )
    }


    @ExperimentalCoroutinesApi
    @FlowPreview
    @EngineAPI
    @InternalAPI
    fun run() {
        logger.info { "Starting the box" }

        liveness = true

        startServer()

        readiness = true

        awaitShutdown(lock, condition)
    }

    @ExperimentalCoroutinesApi
    @FlowPreview
    @EngineAPI
    @InternalAPI
    private fun startServer() {
        
        context.keepAliveHandler.start()
        context.timeoutHandler.start()
        
        HttpServer(context).run()
    }

    private fun configureShutdownHook(resources: Deque<AutoCloseable>, lock: ReentrantLock, condition: Condition) {
        Runtime.getRuntime().addShutdownHook(thread(
            start = false,
            name = "Shutdown hook"
        ) {
            logger.info { "Shutdown start" }
            readiness = false
            try {
                lock.lock()
                condition.signalAll()
            } finally {
                lock.unlock()
            }
            resources.descendingIterator().forEachRemaining { resource ->
                try {
                    resource.close()
                } catch (e: Exception) {
                    logger.error(e) { "Cannot close resource ${resource::class}" }
                }
            }
            liveness = false
            logger.info { "Shutdown end" }
        })
    }

    @Throws(InterruptedException::class)
    private fun awaitShutdown(lock: ReentrantLock, condition: Condition) {
        try {
            lock.lock()
            logger.info { "Wait shutdown" }
            condition.await()
            logger.info { "App shutdown" }
        } finally {
            lock.unlock()
        }
    }
}


@FlowPreview
@EngineAPI
@InternalAPI
@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    try {
        Main(args).run()
    } catch (ex: Exception) {
        logger.error(ex) { "Cannot start the box" }
        exitProcess(1)
    }
}
