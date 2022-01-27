/*******************************************************************************
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.lwdataprovider.*
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.producers.MessageProducer53
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong


class MessageSseRequestContext (
    override val channelMessages: SseResponseHandler,
    requestParameters: Map<String, Any> = emptyMap(),
    counter: AtomicLong = AtomicLong(0L),
    scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo(),
    requestedMessages: MutableMap<String, RequestedMessageDetails> = ConcurrentHashMap(),
    val jsonFormatter: CustomJsonFormatter = CustomJsonFormatter()
) : MessageRequestContext(channelMessages, requestParameters, counter, scannedObjectInfo, requestedMessages) {


    override fun createMessageDetails(id: String, time: Long, storedMessage: StoredMessage) : RequestedMessageDetails {
        return SseRequestedMessageDetails(id, time, storedMessage, this)
    }

}

class SseRequestedMessageDetails(
    id: String,
    time: Long,
    storedMessage: StoredMessage,
    override val context: MessageSseRequestContext,
    parsedMessage: List<Message>? = null,
    rawMessage: RawMessage? = null
) : RequestedMessageDetails(id, time, storedMessage, context, parsedMessage, rawMessage) {

    override fun responseMessage() {
        val msg = MessageProducer53.createMessage(this, context.jsonFormatter)
        val event = context.channelMessages.responseBuilder.build(msg, this.context.counter)
        context.channelMessages.buffer.put(event)
    }

}

class SseEventRequestContext (
    override val channelMessages: SseResponseHandler,
    requestParameters: Map<String, Any> = emptyMap(),
    counter: AtomicLong = AtomicLong(0L),
    scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()
) : RequestContext(channelMessages, requestParameters, counter, scannedObjectInfo) {

    fun processEvent(event: Event) {
        val sseEvent = channelMessages.responseBuilder.build(event, counter)
        channelMessages.buffer.put(sseEvent)
        scannedObjectInfo.update(event.eventId, System.currentTimeMillis(), counter)
    }

}