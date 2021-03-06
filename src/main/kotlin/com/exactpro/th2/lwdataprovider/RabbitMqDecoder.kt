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

import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.workers.CodecMessageListener
import com.exactpro.th2.lwdataprovider.workers.DecodeQueueBuffer
import mu.KotlinLogging

class RabbitMqDecoder(private val configuration: Configuration, 
                      private val messageRouterParsedBatch: MessageRouter<MessageBatch>,
                      private val messageRouterRawBatch: MessageRouter<RawMessageBatch>) {
    
    var decodeBuffer = DecodeQueueBuffer(configuration.maxBufferDecodeQueue)
    var parsedMonitor = messageRouterParsedBatch.subscribeAll(CodecMessageListener(decodeBuffer), "from_codec")


    companion object {
        private val logger = KotlinLogging.logger { }
    }
    
    fun sendBatchMessage(batch: RawMessageBatch, session: String) {
        this.messageRouterRawBatch.send(batch, session)
    }
    
    fun registerMessage(message: RequestedMessageDetails) {
        this.decodeBuffer.add(message)
    }

}