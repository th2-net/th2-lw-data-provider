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
import com.exactpro.th2.lwdataprovider.configuration.Mode
import com.exactpro.th2.lwdataprovider.grpc.GrpcDataProviderBackPressure
import com.exactpro.th2.lwdataprovider.grpc.GrpcDataProviderImpl
import com.exactpro.th2.lwdataprovider.grpc.GrpcServer
import com.exactpro.th2.lwdataprovider.http.HttpServer
import io.grpc.BindableService
import io.ktor.server.engine.*
import io.ktor.util.*
import kotlinx.atomicfu.locks.ReentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Condition
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
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
            messageRouterParsedBatch = configurationFactory.messageRouterMessageGroupBatch.also {
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

        resources += AutoCloseable {  context.keepAliveHandler.stop() }
        resources += AutoCloseable {  context.timeoutHandler.stop() }

        @Suppress("LiftReturnOrAssignment")
        when (context.configuration.mode) {
            Mode.HTTP ->  {
                val httpServer = HttpServer(context)
                httpServer.run()
                resources += AutoCloseable { httpServer.stop() }
            }
            Mode.GRPC -> {
                val grpcServer = GrpcServer.createGrpc(context, this.configurationFactory.grpcRouter)
                resources += AutoCloseable {
                    grpcServer.stop()
                    grpcServer.blockUntilShutdown()
                }
            }
        }


    }

    private fun configureShutdownHook(resources: Deque<AutoCloseable>, lock: ReentrantLock, condition: Condition) {
        Runtime.getRuntime().addShutdownHook(thread(
            start = false,
            name = "Shutdown hook"
        ) {
            logger.info { "Shutdown start" }
            readiness = false
            lock.withLock { condition.signalAll() }
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
        lock.withLock {
            logger.info { "Wait shutdown" }
            condition.await()
            logger.info { "App shutdown" }
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
