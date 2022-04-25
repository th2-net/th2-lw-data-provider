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
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.RabbitMqDecoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import mu.KotlinLogging
import java.time.Instant
import java.util.LinkedList
import java.util.Queue
import kotlin.concurrent.withLock
import kotlin.system.measureTimeMillis

class CradleMessageExtractor(configuration: Configuration, private val cradleManager: CradleManager,
                             private val decoder: RabbitMqDecoder) {

    private val storage = cradleManager.storage
    private val batchSize = configuration.batchSize
    private val groupBufferSize = configuration.groupRequestBuffer
    
    companion object {
        private val logger = KotlinLogging.logger { }
        private val TIMESTAMP_COMPARATOR: Comparator<StoredMessage> = Comparator.comparing { it.timestamp }
    }

    fun getStreams(): Collection<String> = storage.streams
    
    fun getMessages(filter: StoredMessageFilter, requestContext: MessageRequestContext) {

        var msgCount = 0
        val time = measureTimeMillis { 
            logger.info { "Executing query $filter" }
            val iterable = getMessagesFromCradle(filter, requestContext);
            val sessionName = filter.streamName.value

            val builder = MessageGroupBatch.newBuilder()
            var msgBufferCount = 0
            val messageBuffer = ArrayList<RequestedMessageDetails>()

            var msgId: StoredMessageId? = null
            for (storedMessage in iterable) {

                if (!requestContext.contextAlive) {
                    return;
                }

                msgId = storedMessage.id
                val tmp = requestContext.createRequestAndAddToBatch(storedMessage, builder)
                messageBuffer.add(tmp)
                ++msgBufferCount

                if (msgBufferCount >= batchSize) {
                    val sendingTime = System.currentTimeMillis()
                    messageBuffer.forEach {
                        it.time = sendingTime
                        decoder.registerMessage(it)
                        requestContext.registerMessage(it)
                    }
                    decoder.sendBatchMessage(builder.build(), sessionName)

                    messageBuffer.clear()
                    builder.clear()
                    msgCount += msgBufferCount
                    logger.debug { "Message batch sent ($msgBufferCount). Total messages $msgCount" }
                    decoder.decodeBuffer.checkAndWait()
                    requestContext.checkAndWaitForRequestLimit(msgBufferCount)
                    msgBufferCount = 0
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

            requestContext.streamInfo.registerMessage(msgId)
            requestContext.loadedMessages += msgCount
        }

        logger.info { "Loaded $msgCount messages from DB $time ms"}

    }

    private fun MessageRequestContext.createRequestAndAddToBatch(
        storedMessage: StoredMessage,
        builder: MessageGroupBatch.Builder
    ): RequestedMessageDetails {
        val id = storedMessage.id.toString()
        val decodingStep = startStep("decoding")
        val tmp = createMessageDetails(id, 0, storedMessage) { decodingStep.finish() }
        tmp.rawMessage = startStep("raw_message_parsing").use { RawMessage.parseFrom(storedMessage.content) }.also {
            builder.addGroupsBuilder() += it
        }
        return tmp
    }

    fun getRawMessages(filter: StoredMessageFilter, requestContext: MessageRequestContext) {

        var msgCount = 0
        val time = measureTimeMillis {
            logger.info { "Executing query $filter" }
            val iterable = getMessagesFromCradle(filter, requestContext);

            val time = System.currentTimeMillis()
            var msgId: StoredMessageId? = null
            for (storedMessageBatch in iterable) {
                if (!requestContext.contextAlive) {
                    return;
                }
                msgId = storedMessageBatch.id
                val id = storedMessageBatch.id.toString()
                val tmp = requestContext.createMessageDetails(id, time, storedMessageBatch)
                tmp.rawMessage = RawMessage.parseFrom(storedMessageBatch.content)
                tmp.responseMessage()
                msgCount++
            }
            requestContext.streamInfo.registerMessage(msgId)
            requestContext.loadedMessages += msgCount
        }

        logger.info { "Loaded $msgCount messages from DB $time ms"}

    }


    fun getMessagesGroup(group: String, start: Instant, end: Instant, requestContext: MessageRequestContext) {
        val messagesGroup = cradleManager.storage.getGroupedMessageBatches(group, start, end)
        val iterator = messagesGroup.iterator()
        var prev: StoredMessageBatch? = null
        if (!iterator.hasNext()) {
            return
        }

        fun StoredMessage.timestampLess(batch: StoredMessageBatch): Boolean = timestamp < batch.firstTimestamp

        var currentBatch: StoredMessageBatch = iterator.next()
        val batchBuilder = MessageGroupBatch.newBuilder()
        val detailsBuffer = arrayListOf<RequestedMessageDetails>()
        val buffer: Queue<StoredMessage> = LinkedList()
        val remaining: Queue<StoredMessage> = LinkedList()
        while (iterator.hasNext()) {
            prev = currentBatch
            currentBatch = iterator.next()
            if (prev.lastTimestamp < currentBatch.firstTimestamp) {
                buffer.addAll(prev.messages)
                tryDrain(group, buffer, detailsBuffer, batchBuilder, requestContext)
            } else {
                remaining.filterTo(buffer) { it.timestampLess(currentBatch) }

                val messageCount = prev.messageCount
                prev.messages.forEachIndexed { index, msg ->
                    if (msg.timestampLess(currentBatch)) {
                        buffer += msg
                    } else {
                        check(remaining.size < groupBufferSize) {
                            "the group buffer size cannot hold all messages: current size $groupBufferSize but needs ${messageCount - index} more"
                        }
                        remaining += msg
                    }
                }
                tryDrain(group, buffer, detailsBuffer, batchBuilder, requestContext)
            }
        }
        if (prev == null) {
            // Only single batch was extracted
            drain(group, currentBatch.messages, detailsBuffer, batchBuilder, requestContext)
        } else {
            drain(
                group,
                ArrayList<StoredMessage>(buffer.size + remaining.size + currentBatch.messageCount).apply {
                    addAll(buffer)
                    addAll(remaining)
                    addAll(currentBatch.messages)
                    sortWith(TIMESTAMP_COMPARATOR)
                },
                detailsBuffer, batchBuilder, requestContext
            )
        }
    }

    private fun tryDrain(
        group: String,
        buffer: Queue<StoredMessage>,
        detailsBuffer: MutableList<RequestedMessageDetails>,
        batchBuilder: MessageGroupBatch.Builder,
        requestContext: MessageRequestContext
    ) {
        val drainBuffer: MutableList<StoredMessage> = ArrayList(batchSize)
        while (buffer.size >= batchSize) {
            val sortedBatch = generateSequence(buffer::poll)
                .take(batchSize)
                .sortedWith(TIMESTAMP_COMPARATOR)
                .toCollection(drainBuffer)
            drain(group, sortedBatch, detailsBuffer, batchBuilder, requestContext)
            drainBuffer.clear()
        }
    }

    private fun drain(
        group: String,
        buffer: Collection<StoredMessage>,
        detailsBuffer: MutableList<RequestedMessageDetails>,
        batchBuilder: MessageGroupBatch.Builder,
        requestContext: MessageRequestContext
    ) {
        fun MessageRequestContext.sendBatch(builder: MessageGroupBatch.Builder, detailsBuf: MutableList<RequestedMessageDetails>) {
            if (detailsBuf.isEmpty()) {
                return
            }
            val messageCount = detailsBuf.size
            val sendingTime = System.currentTimeMillis()
            detailsBuf.forEach {
                it.time = sendingTime
                decoder.registerMessage(it)
                registerMessage(it)
            }
            decoder.sendBatchMessage(builder.build(), group)
            builder.clear()
            detailsBuf.clear()
            checkAndWaitForRequestLimit(messageCount)
        }
        for (message in buffer) {
            detailsBuffer += requestContext.createRequestAndAddToBatch(message, batchBuilder)
            if (detailsBuffer.size == batchSize) {
                requestContext.sendBatch(batchBuilder, detailsBuffer)
            }
        }
        requestContext.sendBatch(batchBuilder, detailsBuffer)
    }

    private fun MessageRequestContext.checkAndWaitForRequestLimit(msgBufferCount: Int) {
        if (maxMessagesPerRequest > 0 && maxMessagesPerRequest <= messagesInProcess.addAndGet(msgBufferCount)) {
            startStep("await_queue").use {
                lock.withLock {
                    condition.await()
                }
            }
        }
    }

    private fun getMessagesFromCradle(filter: StoredMessageFilter, requestContext: MessageRequestContext): Iterable<StoredMessage> =
        requestContext.startStep("cradle").use { storage.getMessages(filter) }

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
            val decodingStep = if (onlyRaw) null else requestContext.startStep("decoding")
            val tmp = requestContext.createMessageDetails(message.id.toString(), time, message) { decodingStep?.finish() }
            tmp.rawMessage = RawMessage.parseFrom(message.content)
            requestContext.loadedMessages += 1
            
            if (onlyRaw) {
                tmp.responseMessage()
            } else {
                val msgBatch = MessageGroupBatch.newBuilder()
                    .apply { addGroupsBuilder() += tmp.rawMessage!! /*TODO: should be refactored*/ }
                    .build()
                decoder.registerMessage(tmp)
                requestContext.registerMessage(tmp)
                decoder.sendBatchMessage(msgBatch, message.streamName)
            }
            
        }

        logger.info { "Loaded 1 messages with id $msgId from DB $time ms"}

    }
}
