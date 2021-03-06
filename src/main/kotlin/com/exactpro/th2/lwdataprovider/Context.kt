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

import com.exactpro.cradle.CradleManager
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.CradleEventExtractor
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import com.exactpro.th2.lwdataprovider.workers.TimerWatcher
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

@Suppress("MemberVisibilityCanBePrivate")
class Context(
    val configuration: Configuration,

    val jacksonMapper: ObjectMapper = jacksonObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .disable(SerializationFeature.INDENT_OUTPUT),

    val cradleManager: CradleManager,
    val messageRouterRawBatch: MessageRouter<RawMessageBatch>,
    val messageRouterParsedBatch: MessageRouter<MessageBatch>,
    val grpcConfig: GrpcConfiguration,
    val keepAliveHandler: KeepAliveHandler = KeepAliveHandler(configuration),
    
    val mqDecoder: RabbitMqDecoder = RabbitMqDecoder(configuration, messageRouterParsedBatch, messageRouterRawBatch),
    val timeoutHandler: TimerWatcher = TimerWatcher(mqDecoder.decodeBuffer, configuration),
    val cradleEventExtractor: CradleEventExtractor = CradleEventExtractor(cradleManager),
    val cradleMsgExtractor: CradleMessageExtractor = CradleMessageExtractor(configuration, cradleManager, mqDecoder),
    
    val pool: ThreadPoolExecutor = Executors.newFixedThreadPool(configuration.execThreadPoolSize) as ThreadPoolExecutor,
    val searchMessagesHandler: SearchMessagesHandler = SearchMessagesHandler(
        cradleMsgExtractor,
        pool
    ),
    val searchEventsHandler: SearchEventsHandler = SearchEventsHandler(cradleEventExtractor, pool)
)
