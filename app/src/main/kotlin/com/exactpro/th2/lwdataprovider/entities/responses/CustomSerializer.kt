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

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.utils.TimeUtils
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.responses.ser.numberOfDigits
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.text.Charsets.UTF_8
import com.exactpro.cradle.utils.EscapeUtils.escape as cradleEscape

private val COMMA = ",".toByteArray(UTF_8).first().toInt()
private val COLON = ":".toByteArray(UTF_8).first().toInt()
private val ZERO = "0".toByteArray(UTF_8).first().toInt()
private val NULL = "null".toByteArray(UTF_8)
private val TRUE = "true".toByteArray(UTF_8)
private val FALSE = "false".toByteArray(UTF_8)
private val OPENING_CURLY_BRACE = "{".toByteArray(UTF_8).first().toInt()
private val CLOSING_CURLY_BRACE = "}".toByteArray(UTF_8).first().toInt()
private val OPENING_SQUARE_BRACE = "[".toByteArray(UTF_8).first().toInt()
private val CLOSING_SQUARE_BRACE = "]".toByteArray(UTF_8).first().toInt()
private val GREATER_THAN = ">".toByteArray(UTF_8).first().toInt()
private val DOUBLE_QUOTE = """"""".toByteArray(UTF_8).first().toInt()

private val TIMESTAMP_FILED = """"timestamp"""".toByteArray(UTF_8)
private val EPOCH_SECOND_FILED = """"epochSecond"""".toByteArray(UTF_8)
private val NANO_FILED = """"nano"""".toByteArray(UTF_8)
private val DIRECTION_FILED = """"direction"""".toByteArray(UTF_8)
private val SESSION_ID_FILED = """"sessionId"""".toByteArray(UTF_8)
private val MESSAGE_ID_FILED = """"messageId"""".toByteArray(UTF_8)
private val ATTACHED_EVENT_IDS_FILED = """"attachedEventIds"""".toByteArray(UTF_8)
private val BODY_FILED = """"body"""".toByteArray(UTF_8)
private val BODY_BASE_64_FILED = """"bodyBase64"""".toByteArray(UTF_8)
private val METADATA_FILED = """"metadata"""".toByteArray(UTF_8)
private val SUBSEQUENCE_FILED = """"subsequence"""".toByteArray(UTF_8)
private val MESSAGE_TYPE_FILED = """"messageType"""".toByteArray(UTF_8)
private val PROPERTIES_FILED = """"properties"""".toByteArray(UTF_8)
private val PROTOCOL_FILED = """"protocol"""".toByteArray(UTF_8)
private val FIELDS_FILED = """"fields"""".toByteArray(UTF_8)

private val EVENT_ID_FILED = """"eventId"""".toByteArray(UTF_8)
private val BATCH_ID_FILED = """"batchId"""".toByteArray(UTF_8)
private val IS_BATCHED_FILED = """"isBatched"""".toByteArray(UTF_8)
private val EVENT_NAME_FILED = """"eventName"""".toByteArray(UTF_8)
private val EVENT_TYPE_FILED = """"eventType"""".toByteArray(UTF_8)
private val END_TIMESTAMP_FILED = """"endTimestamp"""".toByteArray(UTF_8)
private val START_TIMESTAMP_FILED = """"startTimestamp"""".toByteArray(UTF_8)
private val PARENT_EVENT_ID_FILED = """"parentEventId"""".toByteArray(UTF_8)
private val SUCCESSFUL_FILED = """"successful"""".toByteArray(UTF_8)
private val BOOK_ID_FILED = """"bookId"""".toByteArray(UTF_8)
private val SCOPE_FILED = """"scope"""".toByteArray(UTF_8)
private val ATTACHED_MESSAGE_IDS_FILED = """"attachedMessageIds"""".toByteArray(UTF_8)

private val TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSSSSSS")
    .withZone(ZoneOffset.UTC)
private val ESCAPE_CHARACTERS = charArrayOf('\"', '\n', '\r', '\\', '\t', '\b')

private val bufferPool = ByteBufPool()

fun ProviderMessage53Transport.toJSONByteArray(): ByteArray = bufferPool.withBuffer {
        writeByte(OPENING_CURLY_BRACE)
        writeTimestamp(TIMESTAMP_FILED, timestamp)
        writeByte(COMMA)
        direction?.let {
            writeField(DIRECTION_FILED, direction.name)
            writeByte(COMMA)
        }
        writeField(SESSION_ID_FILED, sessionId)
        writeByte(COMMA)
        writeAttachedEventIds(attachedEventIds)
        body?.let {
            writeByte(COMMA)
            writeBody(body)
        }
        bodyBase64?.let {
            writeByte(COMMA)
            writeFieldWithoutEscaping(BODY_BASE_64_FILED, bodyBase64)
        }
        writeByte(COMMA)
        writeMessageId(messageId)
        writeByte(CLOSING_CURLY_BRACE)
    }

fun Event.toJSONByteArray(): ByteArray = bufferPool.withBuffer {
    writeByte(OPENING_CURLY_BRACE)
    writeField(EVENT_ID_FILED, eventId)
    writeByte(COMMA)
    batchId?.let { writeField(BATCH_ID_FILED, it) } ?: run { writeNull(BATCH_ID_FILED) }
    writeByte(COMMA)
    writeField(IS_BATCHED_FILED, isBatched)
    writeByte(COMMA)
    writeField(EVENT_NAME_FILED, eventName)
    writeByte(COMMA)
    eventType?.let { writeField(EVENT_TYPE_FILED, it) } ?: run { writeNull(EVENT_TYPE_FILED) }
    writeByte(COMMA)
    endTimestamp?.let { writeTimestamp(END_TIMESTAMP_FILED, it) } ?: run { writeNull(END_TIMESTAMP_FILED) }
    writeByte(COMMA)
    writeTimestamp(START_TIMESTAMP_FILED, startTimestamp)
    writeByte(COMMA)
    parentEventId?.let { writeBatchParentEventId(PARENT_EVENT_ID_FILED, it) } ?: run {
        writeNull(
            PARENT_EVENT_ID_FILED
        )
    }
    writeByte(COMMA)
    writeField(SUCCESSFUL_FILED, successful)
    writeByte(COMMA)
    writeField(BOOK_ID_FILED, bookId)
    writeByte(COMMA)
    writeField(SCOPE_FILED, scope)
    writeByte(COMMA)
    if (attachedMessageIds.isNotEmpty()) {
        writeStringList(ATTACHED_MESSAGE_IDS_FILED, attachedMessageIds)
    } else {
        writeEmptyList(ATTACHED_MESSAGE_IDS_FILED)
    }
    writeByte(COMMA)
    val eventBody = body
    if (eventBody != null && eventBody.isNotEmpty()) {
        writeBody(BODY_FILED, eventBody)
    } else {
        writeEmptyList(BODY_FILED)
    }
    writeByte(CLOSING_CURLY_BRACE)
}

internal fun jsonEscape(value: String): String {
    val noEscapeRequired = value.chars().noneMatch {
        it.toChar().let { ch ->
            ch in ESCAPE_CHARACTERS || ch < ' ' || ch == '\u007f'
        }
    }
    if (noEscapeRequired) {
        return value
    }
    return buildString(value.length) {
        for (ch in value) {
            when (ch) {
                '\"' -> append("\\\"")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\\' -> append("\\\\")
                '\t' -> append("\\t")
                '\b' -> append("\\b")
                in '\u0000'..'\u000F' ->
                    append("\\u000").append(ch.code.toString(16))
                in '\u0010'..'\u001F' ->
                    append("\\u00").append(ch.code.toString(16))
                // DEL
                0x7F.toChar() -> append("\\u007f")
                else -> append(ch)
            }
        }
    }
}

private fun ByteBuf.writeMessageId(messageId: StoredMessageId) {
    writeBytes(MESSAGE_ID_FILED)
    writeByte(COLON)
    writeByte(DOUBLE_QUOTE)
    with(messageId) {
        writeBytes(jsonEscape(cradleEscape(bookId.toString())).toByteArray(UTF_8))
        writeByte(COLON)
        writeBytes(jsonEscape(cradleEscape(sessionAlias)).toByteArray(UTF_8))
        writeByte(COLON)
        writeBytes(direction.label.toByteArray(UTF_8))
        writeByte(COLON)
        TimeUtils.toLocalTimestamp(timestamp).apply {
            writeNumber(year, 4)
            writeTwoDigits(monthValue)
            writeTwoDigits(dayOfMonth)
            writeTwoDigits(hour)
            writeTwoDigits(minute)
            writeTwoDigits(second)
            writeNumber(nano, 9)
        }
        writeByte(COLON)
        writeBytes(sequence.toString().toByteArray(UTF_8))
    }
    writeByte(DOUBLE_QUOTE)
}

private fun ByteBuf.writeBody(messages: List<TransportMessageContainer>) {
    writeBytes(BODY_FILED)
    writeByte(COLON)
    writeByte(OPENING_SQUARE_BRACE)
    messages.forEachIndexed { index, message ->
        val parsedMessage = message.parsedMessage
        if (!parsedMessage.rawBody.isReadable) {
            error("The ${parsedMessage.id} message can't be serialized because raw data is blank")
        }
        if (index != 0) {
            writeByte(COMMA)
        }
        writeByte(OPENING_CURLY_BRACE)
        writeMetadata(parsedMessage)
        writeByte(COMMA)
        writeFields(parsedMessage)
        writeByte(CLOSING_CURLY_BRACE)
    }
    writeByte(CLOSING_SQUARE_BRACE)
}

private fun ByteBuf.writeFields(parsedMessage: ParsedMessage) {
    writeBytes(FIELDS_FILED)
    writeByte(COLON)
    writeBytes(parsedMessage.rawBody.toByteArray())
}

private fun ByteBuf.writeMetadata(message: ParsedMessage) {
    writeBytes(METADATA_FILED)
    writeByte(COLON)
    writeByte(OPENING_CURLY_BRACE)

    with(message) {
        if (id.subsequence.isNotEmpty()) {
            writeNumberList(SUBSEQUENCE_FILED, id.subsequence)
            writeByte(COMMA)
        }
        writeField(MESSAGE_TYPE_FILED, type)
        if (metadata.isNotEmpty()) {
            writeByte(COMMA)
            writeMap(PROPERTIES_FILED, metadata)
        }
        if (protocol.isNotBlank()) {
            writeByte(COMMA)
            writeField(PROTOCOL_FILED, protocol)
        }
    }

    writeByte(CLOSING_CURLY_BRACE)
}

private fun ByteBuf.writeBatchParentEventId(name: ByteArray, batchEventId: ProviderEventId) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(DOUBLE_QUOTE)
    batchEventId.batchId?.let {
        writeEventId(it)
        writeByte(GREATER_THAN)
    }
    writeEventId(batchEventId.eventId)
    writeByte(DOUBLE_QUOTE)
}

private fun ByteBuf.writeBody(name: ByteArray, value: ByteArray) {
    writeBytes(name)
    writeByte(COLON)
    if (value.first().toInt().let { it == OPENING_SQUARE_BRACE || it == OPENING_CURLY_BRACE }
        && value.last().toInt().let { it == CLOSING_SQUARE_BRACE || it == CLOSING_CURLY_BRACE }) {
        writeBytes(value)
    } else {
        writeByte(DOUBLE_QUOTE)
        writeBytes(Base64.getEncoder().encode(value))
        writeByte(DOUBLE_QUOTE)
    }
}

private fun ByteBuf.writeEventId(eventId: StoredTestEventId) {
    writeBytes(jsonEscape(eventId.bookId.name).toByteArray(UTF_8))
    writeByte(COLON)
    writeBytes(jsonEscape(eventId.scope).toByteArray(UTF_8))
    writeByte(COLON)
    writeBytes(TIMESTAMP_FORMAT.format(LocalDateTime.ofInstant(eventId.startTimestamp, ZoneOffset.UTC)).toByteArray(UTF_8))
    writeByte(COLON)
    writeBytes(jsonEscape(eventId.id).toByteArray(UTF_8))
}

private fun ByteBuf.writeAttachedEventIds(attachedEventIds: Set<String>) {
    writeBytes(ATTACHED_EVENT_IDS_FILED)
    writeByte(COLON)
    writeByte(OPENING_SQUARE_BRACE)
    attachedEventIds.forEachIndexed { index, eventId ->
        if (index != 0) {
            writeByte(COMMA)
        }
        writeByte(DOUBLE_QUOTE)
        writeBytes(jsonEscape(eventId).toByteArray(UTF_8))
        writeByte(DOUBLE_QUOTE)
    }
    writeByte(CLOSING_SQUARE_BRACE)

}

private fun ByteBuf.writeTimestamp(name: ByteArray, timestamp: Instant) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(OPENING_CURLY_BRACE)
    writeField(EPOCH_SECOND_FILED, timestamp.epochSecond)
    writeByte(COMMA)
    writeField(NANO_FILED, timestamp.nano)
    writeByte(CLOSING_CURLY_BRACE)
}

private fun ByteBuf.writeTwoDigits(value: Int) {
    if (value < 10) {
        writeByte(ZERO)
    }
    writeBytes(value.toString().toByteArray(UTF_8))
}

private fun ByteBuf.writeNumber(value: Int, size: Int) {
    val digits = numberOfDigits(value)
    if (digits < size) {
        repeat(size - digits) {
            writeByte(ZERO)
        }
    }
    writeBytes(value.toString().toByteArray(UTF_8))
}

private fun ByteBuf.writeFieldWithoutEscaping(name: ByteArray, value: String) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(DOUBLE_QUOTE)
    writeBytes(value.toByteArray(UTF_8))
    writeByte(DOUBLE_QUOTE)
}

private fun ByteBuf.writeField(name: ByteArray, value: String) = writeFieldWithoutEscaping(name, jsonEscape(value))
private fun ByteBuf.writeField(name: ByteArray, value: Boolean) {
    writeBytes(name)
    writeByte(COLON)
    writeBytes(if(value) TRUE else FALSE)
}
private fun ByteBuf.writeField(name: ByteArray, value: Number) {
    writeBytes(name)
    writeByte(COLON)
    writeBytes(value.toString().toByteArray(UTF_8))
}

private fun ByteBuf.writeField(name: String, value: String) {
    writeByte(DOUBLE_QUOTE)
    writeBytes(jsonEscape(name).toByteArray(UTF_8))
    writeByte(DOUBLE_QUOTE)
    writeByte(COLON)
    writeByte(DOUBLE_QUOTE)
    writeBytes(jsonEscape(value).toByteArray(UTF_8))
    writeByte(DOUBLE_QUOTE)
}

private fun ByteBuf.writeNull(name: ByteArray) {
    writeBytes(name)
    writeByte(COLON)
    writeBytes(NULL)
}

private fun ByteBuf.writeEmptyList(name: ByteArray) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(OPENING_SQUARE_BRACE)
    writeByte(CLOSING_SQUARE_BRACE)
}

private fun ByteBuf.writeMap(name: ByteArray, value: Map<String, String>) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(OPENING_CURLY_BRACE)
    value.onEachIndexed { index, entry ->
        if (index != 0) {
            writeByte(COMMA)
        }
        writeField(entry.key, entry.value)
    }
    writeByte(CLOSING_CURLY_BRACE)
}

private fun ByteBuf.writeNumberList(name: ByteArray, value: Collection<Number>) {
    writeList(name, value) { writeBytes(it.toString().toByteArray(UTF_8)) }
}

private fun ByteBuf.writeStringList(name: ByteArray, value: Collection<String>) {
    writeList(name, value) {
        writeByte(DOUBLE_QUOTE)
        writeBytes(jsonEscape(it).toByteArray(UTF_8))
        writeByte(DOUBLE_QUOTE)
    }
}

private fun <T> ByteBuf.writeList(name: ByteArray, values: Collection<T>, writeValue: ByteBuf.(T) -> Unit) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(OPENING_SQUARE_BRACE)
    val lastIndex = values.size - 1
    values.forEachIndexed { index, value ->
        writeValue(value)
        if (lastIndex != index) {
            writeByte(COMMA)
        }
    }
    writeByte(CLOSING_SQUARE_BRACE)
}

private class ByteBufPool(
    private val bufferSize: Int = 1_024 * 2,
    private val maxPoolSize: Int = 10
) {
    private val pool = ConcurrentLinkedQueue<ByteBuf>()

    inline fun withBuffer(builder: ByteBuf.() -> Unit): ByteArray {
        val buf = pool.poll()?.clear() ?: Unpooled.buffer(bufferSize)
        try {
            buf.builder()
            return buf.toByteArray()
        } finally {
            if (pool.size < maxPoolSize) {
                pool.offer(buf.clear())
            } else {
                buf.release()
            }
        }
    }
}