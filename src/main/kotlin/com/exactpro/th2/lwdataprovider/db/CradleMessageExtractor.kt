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

package com.exactpro.th2.lwdataprovider.db

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.RabbitMqDecoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import mu.KotlinLogging
import kotlin.system.measureTimeMillis

class CradleMessageExtractor(configuration: Configuration, private val cradleManager: CradleManager,
                             private val decoder: RabbitMqDecoder) {

    private val storage = cradleManager.storage
    private val batchSize = configuration.batchSize
    
    companion object {
        private val logger = KotlinLogging.logger { }
    }
    
    fun getMessages(filter: StoredMessageFilter, requestContext: MessageRequestContext) {

        var msgCount = 0L
        val time = measureTimeMillis { 
            logger.info { "Executing query $filter" }
            val iterable = storage.getMessages(filter);
            val sessionName = filter.streamName.value

            var builder = RawMessageBatch.newBuilder()
            var msgBufferCount = 0
            val messageBuffer = ArrayList<RequestedMessageDetails>()
            for (storedMessageBatch in iterable) {
                
                val id = storedMessageBatch.id.toString()
                val tmp = RequestedMessageDetails(id, 0, storedMessageBatch, requestContext)
                messageBuffer.add(tmp)
                ++msgBufferCount
                tmp.rawMessage = RawMessage.parseFrom(storedMessageBatch.content)
                builder.addMessages(tmp.rawMessage)

                if (msgBufferCount >= batchSize) {
                    val sendingTime = System.currentTimeMillis()
                    messageBuffer.forEach {
                        it.time = sendingTime
                        decoder.registerMessage(it)
                        requestContext.registerMessage(it)
                    }
                    decoder.sendBatchMessage(builder.build(), sessionName)
                    messageBuffer.clear()
                    builder = RawMessageBatch.newBuilder()
                    msgCount += msgBufferCount
                    msgBufferCount = 0
                    decoder.decodeBuffer.checkAndWait()
                }
            }
            
            if (msgBufferCount > 0) {
                decoder.sendBatchMessage(builder.build(), sessionName)
                val sendingTime = System.currentTimeMillis()
                messageBuffer.forEach { 
                    it.time = sendingTime
                    decoder.registerMessage(it)
                    requestContext.registerMessage(it)
                }
                msgCount += msgBufferCount
            }
        }

        logger.info { "Loaded $msgCount messages from DB $time ms"}

    }


    fun getRawMessages(filter: StoredMessageFilter, requestContext: MessageRequestContext) {

        var msgCount = 0L
        val time = measureTimeMillis {
            logger.info { "Executing query $filter" }
            val iterable = storage.getMessages(filter);

            val time = System.currentTimeMillis()
            for (storedMessageBatch in iterable) {
                val id = storedMessageBatch.id.toString()
                val tmp = RequestedMessageDetails(id, time, storedMessageBatch, requestContext)
                tmp.rawMessage = RawMessage.parseFrom(storedMessageBatch.content)
                tmp.responseMessage53()
                msgCount++
            }
        }

        logger.info { "Loaded $msgCount messages from DB $time ms"}

    }

    fun getMessage(msgId: StoredMessageId, onlyRaw: Boolean, requestContext: MessageRequestContext) {
        
        val time = measureTimeMillis {
            logger.info { "Extracting message: $msgId" }
            val message = storage.getMessage(msgId);
            
            if (message == null) {
                requestContext.writeErrorMessage("Message with id $msgId not found")
                requestContext.finishStream()
                return
            }

            val time = System.currentTimeMillis()
            val tmp = RequestedMessageDetails(message.id.toString(), time, message, requestContext)
            tmp.rawMessage = RawMessage.parseFrom(message.content)
            
            if (onlyRaw) {
                tmp.responseMessage53()
            } else {
                val msgBatch = RawMessageBatch.newBuilder().addMessages(tmp.rawMessage).build()
                decoder.registerMessage(tmp)
                requestContext.registerMessage(tmp)
                decoder.sendBatchMessage(msgBatch, message.streamName)
            }
            
        }

        logger.info { "Loaded 1 messages with id $msgId from DB $time ms"}

    }
}
