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

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.EventTreeNode
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.concurrent.atomic.AtomicLong

/**
 * The data class representing an SSE Event that will be sent to the client.
 */

enum class EventType {
    MESSAGE, EVENT, CLOSE, ERROR, KEEP_ALIVE, MESSAGE_IDS;

    override fun toString(): String {
        return super.toString().toLowerCase()
    }
}

data class SseEvent(val data: String = "empty data", val event: EventType? = null, val metadata: String? = null) {
    companion object {
        fun build(jacksonMapper: ObjectMapper, event: EventTreeNode, counter: AtomicLong): SseEvent {
            return SseEvent(
                jacksonMapper.writeValueAsString(event),
                EventType.EVENT,
                counter.incrementAndGet().toString()
            )
        }

        fun build(jacksonMapper: ObjectMapper, event: Event, counter: AtomicLong): SseEvent {
            return SseEvent(
                jacksonMapper.writeValueAsString(event),
                EventType.EVENT,
                counter.incrementAndGet().toString()
            )
        }

        fun build(jacksonMapper: ObjectMapper, message: ProviderMessage, counter: AtomicLong): SseEvent {
            return SseEvent(
                jacksonMapper.writeValueAsString(message),
                EventType.MESSAGE,
                counter.incrementAndGet().toString()
            )
        }

        fun build(
            jacksonMapper: ObjectMapper,
            lastScannedObjectInfo: LastScannedObjectInfo,
            counter: AtomicLong
        ): SseEvent {
            return SseEvent(
                data = jacksonMapper.writeValueAsString(lastScannedObjectInfo),
                event = EventType.KEEP_ALIVE,
                metadata = counter.incrementAndGet().toString()
            )
        }
        
        fun build(jacksonMapper: ObjectMapper, e: Exception): SseEvent {
            var rootCause: Throwable? = e
            while (rootCause?.cause != null) {
                rootCause = rootCause.cause
            }
            return SseEvent(
                jacksonMapper.writeValueAsString(ExceptionInfo(e.javaClass.name,rootCause?.message ?: e.toString())),
                event = EventType.ERROR
            )
        }

        fun build(
            jacksonMapper: ObjectMapper,
            lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>
        ): SseEvent {
            return SseEvent(
                jacksonMapper.writeValueAsString(
                    mapOf(
                        "messageIds" to lastIdInStream.entries.associate { it.key to it.value?.toString() }
                    )
                ),
                event = EventType.MESSAGE_IDS
            )
        }
    }
}

data class ExceptionInfo(val exceptionName: String, val exceptionCause: String)