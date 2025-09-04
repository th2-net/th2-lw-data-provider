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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.lwdataprovider.Escaper
import com.exactpro.th2.lwdataprovider.entities.responses.ser.numberOfDigits
import java.io.OutputStream
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneOffset
import java.util.*
import kotlin.text.Charsets.UTF_8
import com.exactpro.cradle.utils.EscapeUtils.escape as cradleEscape

private val COMMA = ",".toByteArray(UTF_8).first().toInt()
private val COLON = ":".toByteArray(UTF_8).first().toInt()
private val ZERO = "0".toByteArray(UTF_8).first().toInt()
private val ZERO2 = "00".toByteArray(UTF_8)
private val ZERO3 = "000".toByteArray(UTF_8)
private val ZERO4 = "0000".toByteArray(UTF_8)
private val ZERO5 = "00000".toByteArray(UTF_8)
private val ZERO6 = "000000".toByteArray(UTF_8)
private val ZERO7 = "0000000".toByteArray(UTF_8)
private val ZERO8 = "00000000".toByteArray(UTF_8)
private val NULL = "null".toByteArray(UTF_8)
private val TRUE = "true".toByteArray(UTF_8)
private val FALSE = "false".toByteArray(UTF_8)
private val OPENING_CURLY_BRACE = "{".toByteArray(UTF_8).first().toInt()
private val CLOSING_CURLY_BRACE = "}".toByteArray(UTF_8).first().toInt()
private val OPENING_SQUARE_BRACE = "[".toByteArray(UTF_8).first().toInt()
private val CLOSING_SQUARE_BRACE = "]".toByteArray(UTF_8).first().toInt()
private val GREATER_THAN = ">".toByteArray(UTF_8).first().toInt()
private val DOUBLE_QUOTE = """"""".toByteArray(UTF_8).first().toInt()
private val DIVIDER = ">".toByteArray(UTF_8).first().toInt()

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

fun ProviderMessage53Transport.writeJsonData(out: OutputStream, escaper: Escaper): Unit = with(out) {
    write(OPENING_CURLY_BRACE)
    writeTimestamp(TIMESTAMP_FILED, timestamp)
    write(COMMA)
    direction?.let {
        writeFieldWithoutEscaping(DIRECTION_FILED, direction.name.toByteArray(UTF_8))
        write(COMMA)
    }
    writeFieldWithoutEscaping(SESSION_ID_FILED, escaper.escape(sessionId, true))
    write(COMMA)
    writeAttachedEventIds(attachedEventIds, escaper)
    body?.let {
        write(COMMA)
        writeBody(body, escaper)
    }
    bodyBase64?.let {
        write(COMMA)
        writeFieldWithoutEscaping(BODY_BASE_64_FILED, bodyBase64.toByteArray(UTF_8))
    }
    write(COMMA)
    writeMessageIdField(MESSAGE_ID_FILED, messageId, escaper)
    write(CLOSING_CURLY_BRACE)
}

fun LwEvent.writeJsonData(out: OutputStream, escaper: Escaper): Unit = with(out) {
    write(OPENING_CURLY_BRACE)
    writeEventIdField(EVENT_ID_FILED, batchId, eventId, escaper)
    write(COMMA)
    batchId?.let { writeEventIdField(BATCH_ID_FILED, null, it, escaper) } ?: run { writeNull(BATCH_ID_FILED) }
    write(COMMA)
    writeField(IS_BATCHED_FILED, isBatched)
    write(COMMA)
    writeField(EVENT_NAME_FILED, event.name, escaper)
    write(COMMA)
    event.type?.let { writeField(EVENT_TYPE_FILED, it, escaper) } ?: run { writeNull(EVENT_TYPE_FILED) }
    write(COMMA)
    event.endTimestamp?.let { writeTimestamp(END_TIMESTAMP_FILED, it) } ?: run { writeNull(END_TIMESTAMP_FILED) }
    write(COMMA)
    writeTimestamp(START_TIMESTAMP_FILED, event.id.startTimestamp)
    write(COMMA)
    event.parentId
        ?.let { writeEventIdField(PARENT_EVENT_ID_FILED, parentBatchId, it, escaper) }
        ?: run { writeNull(PARENT_EVENT_ID_FILED) }
    write(COMMA)
    writeField(SUCCESSFUL_FILED, event.isSuccess)
    write(COMMA)
    writeFieldWithoutEscaping(BOOK_ID_FILED, escaper.escape(bookId, true))
    write(COMMA)
    writeFieldWithoutEscaping(SCOPE_FILED, escaper.escape(scope, true))
    write(COMMA)
    if (attachedMessageIds.isNotEmpty()) {
        writeMessageIdList(ATTACHED_MESSAGE_IDS_FILED, attachedMessageIds, escaper)
    } else {
        writeEmptyList(ATTACHED_MESSAGE_IDS_FILED)
    }
    write(COMMA)
    if (event.content != null && event.content.remaining() > 0) {
        writeBody(BODY_FILED, event.content)
    } else {
        writeEmptyList(BODY_FILED)
    }
    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeMessageId(messageId: StoredMessageId, escaper: Escaper) {
    with(messageId) {
        write(escaper.escape(cradleEscape(bookId.toString()), true))
        write(COLON)
        write(escaper.escape(cradleEscape(sessionAlias), true))
        write(COLON)
        write(direction.label.toByteArray(UTF_8))
        write(COLON)
        writeTimestamp(timestamp)
        write(COLON)
        write(sequence.toString().toByteArray(UTF_8))
    }
}

private fun OutputStream.writeMessageIdField(name: ByteArray, messageId: StoredMessageId, escaper: Escaper) {
    write(name)
    write(COLON)
    write(DOUBLE_QUOTE)
    writeMessageId(messageId, escaper)
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeMessageIdList(name: ByteArray, messageIds: Collection<StoredMessageId>, escaper: Escaper) {
    writeList(name, messageIds) {
        write(DOUBLE_QUOTE)
        writeMessageId(it, escaper)
        write(DOUBLE_QUOTE)
    }
}

private fun OutputStream.writeBody(messages: List<TransportMessageContainer>, escaper: Escaper) {
    write(BODY_FILED)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    messages.forEachIndexed { index, message ->
        val parsedMessage = message.parsedMessage
        if (!parsedMessage.rawBody.isReadable) {
            error("The ${parsedMessage.id} message can't be serialized because raw data is blank")
        }
        if (index != 0) {
            write(COMMA)
        }
        write(OPENING_CURLY_BRACE)
        writeMetadata(parsedMessage, escaper)
        write(COMMA)
        writeFields(parsedMessage)
        write(CLOSING_CURLY_BRACE)
    }
    write(CLOSING_SQUARE_BRACE)
}

private fun OutputStream.writeFields(parsedMessage: ParsedMessage) {
    write(FIELDS_FILED)
    write(COLON)
    write(parsedMessage.rawBody.toByteArray())
}

private fun OutputStream.writeMetadata(message: ParsedMessage, escaper: Escaper) {
    write(METADATA_FILED)
    write(COLON)
    write(OPENING_CURLY_BRACE)

    with(message) {
        if (id.subsequence.isNotEmpty()) {
            writeNumberList(SUBSEQUENCE_FILED, id.subsequence)
            write(COMMA)
        }
        writeField(MESSAGE_TYPE_FILED, type, escaper)
        if (metadata.isNotEmpty()) {
            write(COMMA)
            writeMap(PROPERTIES_FILED, metadata, escaper)
        }
        if (protocol.isNotBlank()) {
            write(COMMA)
            writeFieldWithoutEscaping(PROTOCOL_FILED, escaper.escape(protocol, true))
        }
    }

    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeBody(name: ByteArray, value: ByteBuffer) {
    write(name)
    write(COLON)
    val first = value.get(value.position()).toInt()
    val last = value.get(value.limit() - 1).toInt()
    if (first.let { it == OPENING_SQUARE_BRACE || it == OPENING_CURLY_BRACE }
        && last.let { it == CLOSING_SQUARE_BRACE || it == CLOSING_CURLY_BRACE }) { // TODO: use single write
        write(
            value.array(),
            value.arrayOffset() + value.position(),
            value.remaining()
        )
    } else {
        write(DOUBLE_QUOTE)
        Base64.getEncoder().wrap(this).write(
            value.array(),
            value.arrayOffset() + value.position(),
            value.remaining()
        )
        write(DOUBLE_QUOTE)
    }
}

private fun OutputStream.writeEventId(eventId: StoredTestEventId, escaper: Escaper) {
    write(escaper.escape(eventId.bookId.name, true))
    write(COLON)
    write(escaper.escape(eventId.scope, true))
    write(COLON)
    writeTimestamp(eventId.startTimestamp)
    write(COLON)
    write(escaper.escape(eventId.id, false))
}

private fun OutputStream.writeTimestamp(timestamp: Instant) {
    timestamp.atZone(ZoneOffset.UTC).apply {
        writeFourDigits(year)
        writeTwoDigits(monthValue)
        writeTwoDigits(dayOfMonth)
        writeTwoDigits(hour)
        writeTwoDigits(minute)
        writeTwoDigits(second)
        writeNineDigits(nano)
    }
}

private fun OutputStream.writeEventIdField(name: ByteArray, batchEventId: StoredTestEventId?, eventId: StoredTestEventId, escaper: Escaper) {
    write(name)
    write(COLON)
    write(DOUBLE_QUOTE)
    if (batchEventId != null) {
        writeEventId(batchEventId, escaper)
        write(DIVIDER)
    }
    writeEventId(eventId, escaper)
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeAttachedEventIds(attachedEventIds: Set<String>, escaper: Escaper) {
    write(ATTACHED_EVENT_IDS_FILED)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    attachedEventIds.forEachIndexed { index, eventId ->
        if (index != 0) {
            write(COMMA)
        }
        write(DOUBLE_QUOTE)
        write(escaper.escape(eventId, false))
        write(DOUBLE_QUOTE)
    }
    write(CLOSING_SQUARE_BRACE)

}

private fun OutputStream.writeTimestamp(name: ByteArray, timestamp: Instant) {
    write(name)
    write(COLON)
    write(OPENING_CURLY_BRACE)
    writeField(EPOCH_SECOND_FILED, timestamp.epochSecond)
    write(COMMA)
    writeField(NANO_FILED, timestamp.nano)
    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeFourDigits(value: Int) {
    when {
        value < 10 -> write(ZERO3)
        value < 100 -> write(ZERO2)
        value < 1000 -> write(ZERO)
    }
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeNineDigits(value: Int) {
    when {
        value < 10 -> write(ZERO8)
        value < 100 -> write(ZERO7)
        value < 1000 -> write(ZERO6)
        value < 10000 -> write(ZERO5)
        value < 100000 -> write(ZERO4)
        value < 1000000 -> write(ZERO3)
        value < 10000000 -> write(ZERO2)
        value < 100000000 -> write(ZERO)
    }
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeTwoDigits(value: Int) {
    if (value < 10) {
        write(ZERO)
    }
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeNumber(value: Int, size: Int) {
    val digits = numberOfDigits(value)
    if (digits < size) {
        repeat(size - digits) {
            write(ZERO)
        }
    }
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeFieldWithoutEscaping(name: ByteArray, value: ByteArray) {
    write(name)
    write(COLON)
    write(DOUBLE_QUOTE)
    write(value)
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeField(name: ByteArray, value: String, escaper: Escaper) = writeFieldWithoutEscaping(name, escaper.escape(value, false))
private fun OutputStream.writeField(name: ByteArray, value: Boolean) {
    write(name)
    write(COLON)
    write(if (value) TRUE else FALSE)
}

private fun OutputStream.writeField(name: ByteArray, value: Number) {
    write(name)
    write(COLON)
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeField(name: String, value: String, escaper: Escaper) {
    write(DOUBLE_QUOTE)
    write(escaper.escape(name, true))
    write(DOUBLE_QUOTE)
    write(COLON)
    write(DOUBLE_QUOTE)
    write(escaper.escape(value, false))
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeNull(name: ByteArray) {
    write(name)
    write(COLON)
    write(NULL)
}

private fun OutputStream.writeEmptyList(name: ByteArray) {
    write(name)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    write(CLOSING_SQUARE_BRACE)
}

private fun OutputStream.writeMap(name: ByteArray, value: Map<String, String>, escaper: Escaper) {
    write(name)
    write(COLON)
    write(OPENING_CURLY_BRACE)
    value.onEachIndexed { index, entry ->
        if (index != 0) {
            write(COMMA)
        }
        writeField(entry.key, entry.value, escaper)
    }
    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeNumberList(name: ByteArray, value: Collection<Number>) {
    writeList(name, value) { write(it.toString().toByteArray(UTF_8)) }
}

private fun <T> OutputStream.writeList(name: ByteArray, values: Collection<T>, writeValue: OutputStream.(T) -> Unit) {
    write(name)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    val lastIndex = values.size - 1
    values.forEachIndexed { index, value ->
        writeValue(value)
        if (lastIndex != index) {
            write(COMMA)
        }
    }
    write(CLOSING_SQUARE_BRACE)
}