/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.SseEvent.Companion.DATA_CHARSET
import com.exactpro.th2.lwdataprovider.entities.responses.ser.ByteBufferPool
import com.exactpro.th2.lwdataprovider.entities.responses.ser.DummyBufferPool
import com.exactpro.th2.lwdataprovider.entities.responses.ser.HeapBufferPool
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.entities.responses.LwEvent
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import com.exactpro.th2.lwdataprovider.entities.responses.ResponseMessage
import com.exactpro.th2.lwdataprovider.producers.JsonFormatter
import com.fasterxml.jackson.databind.ObjectMapper

class SseResponseBuilder(
    private val bufPool: ByteBufferPool = DummyBufferPool,
    private val escaper: Escaper = DummyEscaper,
    private val jacksonMapper: ObjectMapper = ObjectMapper(),
    private val responseFactory: (RequestedMessage, JsonFormatter?, Boolean) -> ResponseMessage,
) {

    fun build(
        message: RequestedMessage,
        formatter: JsonFormatter?,
        includeRaw: Boolean,
        counter: Long,
    ): SseEvent {
        return SseEvent.build(bufPool, escaper, jacksonMapper, responseFactory(message, formatter, includeRaw), counter)
    }
    fun build(message: ResponseMessage, counter: Long): SseEvent {
        return SseEvent.build(bufPool, escaper, jacksonMapper, message, counter)
    }

    fun build(lastScannedObjectInfo: LastScannedObjectInfo, counter: Long): SseEvent {
        return SseEvent.build(jacksonMapper, lastScannedObjectInfo, counter)
    }

    fun build(lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>): SseEvent {
        return SseEvent.build(jacksonMapper, lastIdInStream)
    }

    fun build(event: LwEvent, lastEventId: Long): SseEvent {
        return SseEvent.build(bufPool, escaper, event, lastEventId)
    }

    fun build(pageInfo: PageInfo, lastEventId: Long): SseEvent {
        return SseEvent.build(jacksonMapper, pageInfo, lastEventId)
    }

    fun codecTimeoutError(id: StoredMessageId, lastEventId: Long): SseEvent =
        SseEvent.ErrorData.TimeoutError(
            id.failureReason("Codec response wasn't received during timeout").toByteArray(DATA_CHARSET),
            lastEventId.toString()
        )

    fun create(bufferPool: HeapBufferPool, escaper: Escaper): SseResponseBuilder {
        return SseResponseBuilder(bufferPool, escaper, jacksonMapper, responseFactory)
    }
}