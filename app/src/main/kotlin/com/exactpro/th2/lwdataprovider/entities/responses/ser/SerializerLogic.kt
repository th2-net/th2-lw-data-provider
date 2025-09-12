/*
 * Copyright 2023-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.entities.responses.ser

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.entities.internal.Direction.IN
import com.exactpro.th2.lwdataprovider.entities.internal.Direction.OUT
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53Transport
import com.exactpro.th2.lwdataprovider.entities.responses.TransportMessageContainer
import io.javalin.openapi.OpenApiNullable
import io.javalin.openapi.OpenApiPropertyType
import io.javalin.openapi.OpenApiRequired
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneOffset

private val SPACE = ' '.code.toByte()
private val TAB = '\t'.code.toByte()

fun ProviderMessage53Transport.serializeJsonData(serializer: Serializer<*>): Unit = with(serializer) {
    obj {
        filed(EntityField.TIMESTAMP) { timestampObj(timestamp) }.char(JsonChar.COMMA)
        direction?.let {
            filedStr(EntityField.DIRECTION) {
                when (direction) {
                    IN -> str(SpecialString.IN)
                    OUT -> str(SpecialString.OUT)
                }
            }.char(JsonChar.COMMA)
        }
        filedStr(EntityField.SESSION_ID) { escapeStr(sessionId, true) }.char(JsonChar.COMMA)
        filed(EntityField.ATTACHED_EVENT_IDS) {
            arr {
                attachedEventIds.iterate({ char(JsonChar.COMMA) }) {
                    valueStr { escapeStr(it, false) }
                }
            }
        }.char(JsonChar.COMMA)
        body?.let {
            filed(EntityField.BODY) { body(body) }.char(JsonChar.COMMA)
        }
        bodyBase64?.let {
            filedStr(EntityField.BODY_BASE_64) { str(bodyBase64) }.char(JsonChar.COMMA)
        }
        filedStr(EntityField.MESSAGE_ID) { messageId(messageId) }
    }
}

@Suppress("DataClassPrivateConstructor", "DATA_CLASS_COPY_VISIBILITY_WILL_BE_CHANGED_WARNING")
data class EventSchema private constructor(
    //** full event id  */
    val eventId: String,
    @get:OpenApiRequired
    @get:OpenApiPropertyType(definedBy = String::class)
    val batchId: String?,
    /** last part of event id. it doesn't consider in equal and hashCode methods */
    val shortEventId: String,
    val isBatched: Boolean,
    val eventName: String,
    @get:OpenApiRequired
    @get:OpenApiPropertyType(definedBy = String::class)
    val eventType: String?,
    @get:OpenApiRequired
    @get:OpenApiPropertyType(definedBy = Instant::class)
    val endTimestamp: Instant?,
    val startTimestamp: Instant,
    @get:OpenApiRequired
    @get:OpenApiPropertyType(definedBy = String::class)
    val parentEventId: ProviderEventId?,
    val successful: Boolean,
    val bookId: String,
    val scope: String,
    val attachedMessageIds: Set<String>,

    @get:OpenApiRequired
    @get:OpenApiNullable(nullable = false)
    @get:OpenApiPropertyType(definedBy = Array<Any>::class)
    val body: ByteBuffer?
)

/**
 * Serialize [Event] to JSON (the schema is described by [EventSchema] class)
 */
fun Event.serializeJsonData(serializer: Serializer<*>): Unit = with(serializer) {
    obj {
        filedStr(EntityField.EVENT_ID) { compositeEventId(batchId, eventId) }.char(JsonChar.COMMA)
        filed(EntityField.BATCH_ID) {
            batchId?.let { valueStr { simpleEventId(it) } }
                ?: run { str(JsonString.NULL) }
        }.char(JsonChar.COMMA)
        filedBool(EntityField.IS_BATCHED, isBatched).char(JsonChar.COMMA)
        filedStr(EntityField.EVENT_NAME) { escapeStr(event.name, false) }.char(JsonChar.COMMA)
        filed(EntityField.EVENT_TYPE) {
            event.type?.let { valueStr { escapeStr(it, false) } }
                ?: run { str(JsonString.NULL) }
        }.char(JsonChar.COMMA)
        filed(EntityField.END_TIMESTAMP) {
            event.endTimestamp?.let { timestampObj(it) }
                ?: run { str(JsonString.NULL) }
        }.char(JsonChar.COMMA)
        filed(EntityField.START_TIMESTAMP) { timestampObj(event.id.startTimestamp) }.char(JsonChar.COMMA)
        filed(EntityField.PARENT_EVENT_ID) {
            event.parentId?.let { valueStr { compositeEventId(parentBatchId, it) } }
                ?: run { str(JsonString.NULL) }
        }.char(JsonChar.COMMA)
        filedBool(EntityField.SUCCESSFUL, event.isSuccess).char(JsonChar.COMMA)
        filedStr(EntityField.BOOK_ID) { escapeStr(event.id.bookId.name, true) }.char(JsonChar.COMMA)
        filedStr(EntityField.SCOPE) { escapeStr(event.id.scope, true) }.char(JsonChar.COMMA)
        filed(EntityField.ATTACHED_MESSAGE_IDS) {
            arr {
                attachedMessageIds.iterate({ char(JsonChar.COMMA) }) {
                    valueStr { messageId(it) }
                }
            }
        }.char(JsonChar.COMMA)
        filed(EntityField.BODY) {
            if (event.content != null && event.content.remaining() > 0) {
                body(event.content)
            } else {
                arr {  }
            }
        }
    }
}

private fun Serializer<*>.messageId(messageId: StoredMessageId) {
    with(messageId) {
        escapeStr(bookId.name, true).char(JsonChar.COLON)
        escapeStr(sessionAlias, true).char(JsonChar.COLON)
        when(direction) {
            Direction.FIRST -> char(SpecialChar.ONE)
            Direction.SECOND -> char(SpecialChar.TWO)
            else -> escapeStr(direction.label, true)
        }.char(JsonChar.COLON)
        timestampStr(timestamp).char(JsonChar.COLON)
        numAsStr(sequence)
    }
}

private fun Serializer<*>.body(messages: List<TransportMessageContainer>) {
    arr {
        messages.iterate({ char(JsonChar.COMMA) }) {
            val parsedMessage = it.parsedMessage
            if (!parsedMessage.rawBody.isReadable) {
                error("The ${parsedMessage.id} message can't be serialized because raw data is blank")
            }
            obj {
                filed(EntityField.METADATA) {
                    metadata(parsedMessage)
                }.char(JsonChar.COMMA)
                filed(EntityField.FIELDS) {
                    bytes(parsedMessage.rawBody)
                }
            }
        }
    }
}

private fun Serializer<*>.metadata(message: ParsedMessage) {
    obj {
        with(message) {
            if (id.subsequence.isNotEmpty()) {
                filed(EntityField.SUBSEQUENCE) {
                    arr {
                        id.subsequence.iterate({ char(JsonChar.COMMA) }) {
                            numAsStr(it)
                        }
                    }
                }.char(JsonChar.COMMA)
            }
            filedStr(EntityField.MESSAGE_TYPE) { escapeStr(type, false) }
            if (metadata.isNotEmpty()) {
                char(JsonChar.COMMA)
                filed(EntityField.PROPERTIES) {
                    obj {
                        metadata.iterate({ char(JsonChar.COMMA) }) {
                            valueStr { escapeStr(it.key) }
                                .char(JsonChar.COLON)
                                .valueStr { escapeStr(it.value) }
                        }
                    }
                }
            }
            if (protocol.isNotBlank()) {
                char(JsonChar.COMMA)
                filedStr(EntityField.PROTOCOL) { escapeStr(protocol, true) }
            }
        }
    }
}

private fun Serializer<*>.body(value: ByteBuffer) {
    val startPos = value.position()
    val endPos = value.limit() - 1

    fun Iterable<Int>.findValue(): Byte = asSequence().map(value::get).filter { it != SPACE && it != TAB }.first()
    fun Byte.isOpeningBrace() = this == JsonChar.OPENING_SQUARE_BRACE.byte || this == JsonChar.OPENING_CURLY_BRACE.byte
    fun Byte.isClosingBrace() = this == JsonChar.CLOSING_SQUARE_BRACE.byte || this == JsonChar.CLOSING_CURLY_BRACE.byte

    val first = (startPos..endPos).findValue()
    val last = (endPos downTo startPos).findValue()
    if (first.isOpeningBrace() && last.isClosingBrace()) {
        bytes(value)
    } else {
        valueStr {
            value.mark()
            base64Str(value)
            value.reset()
        }
    }
}

private fun Serializer<*>.simpleEventId(eventId: StoredTestEventId) = this.also {
    escapeStr(eventId.bookId.name, true).char(JsonChar.COLON)
    escapeStr(eventId.scope, true).char(JsonChar.COLON)
    timestampStr(eventId.startTimestamp).char(JsonChar.COLON)
    escapeStr(eventId.id)
}

private fun Serializer<*>.timestampStr(timestamp: Instant): Serializer<*> = this.also {
    timestamp.atZone(ZoneOffset.UTC).apply {
        numAsStr(year, NumberLength.FOUR_DIGITS)
        numAsStr(monthValue, NumberLength.TWO_DIGITS)
        numAsStr(dayOfMonth, NumberLength.TWO_DIGITS)
        numAsStr(hour, NumberLength.TWO_DIGITS)
        numAsStr(minute, NumberLength.TWO_DIGITS)
        numAsStr(second, NumberLength.TWO_DIGITS)
        numAsStr(nano, NumberLength.NINE_DIGITS)
    }
}

private fun Serializer<*>.timestampObj(timestamp: Instant) {
    obj {
        filed(EntityField.EPOCH_SECOND) { numAsStr(timestamp.epochSecond) }.char(JsonChar.COMMA)
        filed(EntityField.NANO) { numAsStr(timestamp.nano) }
    }
}

private fun Serializer<*>.compositeEventId(batchEventId: StoredTestEventId?, eventId: StoredTestEventId) {
    if (batchEventId != null) {
        simpleEventId(batchEventId).char(SpecialChar.GREATER_THAN)
    }
    simpleEventId(eventId)
}

private inline fun <T> Collection<T>.iterate(handleGap: () -> Unit, handleValue: (value: T) -> Unit) {
    val lastIndex = size - 1
    forEachIndexed { index, value ->
        handleValue(value)
        if (lastIndex != index) { handleGap() }
    }
}

private inline fun <K, V> Map<K, V>.iterate(handleGap: () -> Unit, handleEntry: (entry: Map.Entry<K, V>) -> Unit) {
    entries.iterate(handleGap, handleEntry)
}