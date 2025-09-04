/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.entities.responses.LwEvent
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53Transport
import com.exactpro.th2.lwdataprovider.entities.responses.ResponseMessage
import com.exactpro.th2.lwdataprovider.entities.responses.writeJsonData
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToStream
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.nio.charset.Charset
import java.util.*
import kotlin.text.Charsets.UTF_8

/**
 * The data class representing an SSE Event that will be sent to the client.
 */

enum class EventType {
    MESSAGE, EVENT, CLOSE, ERROR, KEEP_ALIVE, MESSAGE_IDS, PAGE_INFO;

    val typeName: String = name.lowercase(Locale.getDefault())

    override fun toString(): String = typeName
}

private val EMPTY_DATA: ByteArray = "empty data".toByteArray(DATA_CHARSET)

interface Writeable {
    fun writeData(out: OutputStream, escaper: Escaper = Escaper { value, _ -> value.toByteArray(UTF_8) })
}

sealed class SseEvent(
    val event: EventType,
): Writeable {
    open val metadata: String? = null

    object Closed : SseEvent(EventType.CLOSE) {
        override fun writeData(
            out: OutputStream,
            escaper: Escaper
        ) { out.write(EMPTY_DATA) }
    }

    class EventData(
        val lwEvent: LwEvent,
        override val metadata: String,
    ) : SseEvent(EventType.EVENT) {
        override fun writeData(
            out: OutputStream,
            escaper: Escaper
        ) { lwEvent.writeJsonData(out, escaper) }
    }

    class MessageData(
        private val jacksonMapper: ObjectMapper,
        val message: ResponseMessage,
        override val metadata: String,
    ) : SseEvent(EventType.MESSAGE) {
        override fun writeData(
            out: OutputStream,
            escaper: Escaper
        ) {
            when (message) {
                // FIXME: implement ProviderMessage53
                is ProviderMessage53 -> JSON.encodeToStream(ProviderMessage53.serializer(), message, out)
                is ProviderMessage53Transport -> message.writeJsonData(out, escaper)
                else -> jacksonMapper.writeValue(out, message)
            }
        }
    }

    class KeepAliveData(
        val data: ByteArray,
        override val metadata: String,
    ) : SseEvent(EventType.KEEP_ALIVE) {
        override fun writeData(
            out: OutputStream,
            escaper: Escaper
        ) { out.write(data) }
    }

    class LastMessageIDsData(
        val data: ByteArray,
    ) : SseEvent(EventType.MESSAGE_IDS) {
        override fun writeData(
            out: OutputStream,
            escaper: Escaper
        ) { out.write(data) }
    }

    class PageInfoData(
        val data: ByteArray,
        override val metadata: String,
    ) : SseEvent(EventType.PAGE_INFO) {
        override fun writeData(
            out: OutputStream,
            escaper: Escaper
        ) { out.write(data) }
    }

    sealed class ErrorData : SseEvent(EventType.ERROR) {
        open val data: ByteArray = EMPTY_DATA

        class SimpleError(
            override val data: ByteArray
        ) : ErrorData() {
            override fun writeData(
                out: OutputStream,
                escaper: Escaper
            ) { out.write(data) }
        }

        class TimeoutError(
            override val data: ByteArray,
            override val metadata: String,
        ) : ErrorData() {
            override fun writeData(
                out: OutputStream,
                escaper: Escaper
            ) { out.write(data) }
        }
    }

    companion object {
        val DATA_CHARSET: Charset = Charsets.UTF_8

        @OptIn(ExperimentalSerializationApi::class)
        private val JSON = Json {
            explicitNulls = false
        }

        fun build(event: LwEvent, counter: Long): SseEvent {
            return EventData(
                event,
                counter.toString(),
            )
        }

        fun build(jacksonMapper: ObjectMapper, message: ResponseMessage, counter: Long): SseEvent {
            return MessageData(
                jacksonMapper,
                message,
                counter.toString(),
            )
        }

        fun build(
            jacksonMapper: ObjectMapper,
            lastScannedObjectInfo: LastScannedObjectInfo,
            counter: Long
        ): SseEvent {
            return KeepAliveData(
                jacksonMapper.writeValueAsBytes(lastScannedObjectInfo),
                counter.toString(),
            )
        }

        fun build(jacksonMapper: ObjectMapper, e: Exception): SseEvent {
            var rootCause: Throwable? = e
            while (rootCause?.cause != null) {
                rootCause = rootCause.cause
            }
            return ErrorData.SimpleError(
                jacksonMapper.writeValueAsBytes(ExceptionInfo(e.javaClass.name, rootCause?.message ?: e.toString())),
            )
        }

        fun build(
            jacksonMapper: ObjectMapper,
            lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>
        ): SseEvent {
            return LastMessageIDsData(
                jacksonMapper.writeValueAsBytes(
                    mapOf(
                        "messageIds" to lastIdInStream.entries.associate { it.key to it.value?.toString() }
                    )
                ),
            )
        }

        fun build(jacksonMapper: ObjectMapper, pageInfo: PageInfo, counter: Long): SseEvent {
            return PageInfoData(
                jacksonMapper.writeValueAsBytes(pageInfo),
                counter.toString(),
            )
        }
    }
}

@OptIn(ExperimentalSerializationApi::class)
private fun <T> Json.encodeToByteArray(serializer: SerializationStrategy<T>, value: T): ByteArray {
    return ByteArrayOutputStream(1024 * 2).use {
        encodeToStream(serializer, value, it)
        it
    }.toByteArray()
}

data class ExceptionInfo(val exceptionName: String, val exceptionCause: String)