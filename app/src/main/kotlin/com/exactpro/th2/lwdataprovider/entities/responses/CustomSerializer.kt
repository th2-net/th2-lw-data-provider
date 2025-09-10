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
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.lang.AutoCloseable
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneOffset
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.text.Charsets.UTF_8
import com.exactpro.cradle.utils.EscapeUtils.escape as cradleEscape

private val COMMA = ",".toByteArray(UTF_8).first().toInt()
private val B_COMMA = ",".toByteArray(UTF_8).first()
private val COLON = ":".toByteArray(UTF_8).first().toInt()
private val B_COLON = ":".toByteArray(UTF_8).first()
private val ZERO = "0".toByteArray(UTF_8).first().toInt()
private val B_ZERO = "0".toByteArray(UTF_8).first()
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
private val B_OPENING_CURLY_BRACE = "{".toByteArray(UTF_8).first()
private val CLOSING_CURLY_BRACE = "}".toByteArray(UTF_8).first().toInt()
private val B_CLOSING_CURLY_BRACE = "}".toByteArray(UTF_8).first()
private val OPENING_SQUARE_BRACE = "[".toByteArray(UTF_8).first().toInt()
private val B_OPENING_SQUARE_BRACE = "[".toByteArray(UTF_8).first()
private val CLOSING_SQUARE_BRACE = "]".toByteArray(UTF_8).first().toInt()
private val B_CLOSING_SQUARE_BRACE = "]".toByteArray(UTF_8).first()
private val DOUBLE_QUOTE = """"""".toByteArray(UTF_8).first().toInt()
private val B_DOUBLE_QUOTE = """"""".toByteArray(UTF_8).first()
private val ID_DIVIDER = ">".toByteArray(UTF_8).first().toInt()
private val B_ID_DIVIDER = ">".toByteArray(UTF_8).first()

private val FIRST = '1'.code.toByte()
private val SECOND = '2'.code.toByte()

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

fun ProviderMessage53Transport.writeJsonData(buf: ByteBuf, escaper: Escaper): ByteBuf = buf.apply {
    writeByte(OPENING_CURLY_BRACE)
    writeTimestamp(TIMESTAMP_FILED, timestamp)
    writeByte(COMMA)
    direction?.let {
        writeFieldWithoutEscaping(DIRECTION_FILED, direction.name)
        writeByte(COMMA)
    }
    writeFieldWithoutEscaping(SESSION_ID_FILED, escaper.escape(sessionId, true))
    writeByte(COMMA)
    writeAttachedEventIds(attachedEventIds, escaper)
    body?.let {
        writeByte(COMMA)
        writeBody(body, escaper)
    }
    bodyBase64?.let {
        writeByte(COMMA)
        writeFieldWithoutEscaping(BODY_BASE_64_FILED, bodyBase64)
    }
    writeByte(COMMA)
    writeMessageIdField(MESSAGE_ID_FILED, messageId, escaper)
    writeByte(CLOSING_CURLY_BRACE)
}

fun ProviderMessage53Transport.putJsonData(buffer: ByteBuffer, escaper: Escaper): ByteBuffer = buffer.apply {
    put(B_OPENING_CURLY_BRACE)
    putTimestamp(TIMESTAMP_FILED, timestamp)
    put(B_COMMA)
    direction?.let {
        putFieldWithoutEscaping(DIRECTION_FILED, direction.name.toByteArray(UTF_8)) // TODO: use
        put(B_COMMA)
    }
    putFieldWithoutEscaping(SESSION_ID_FILED, escaper.escape(sessionId, true))
    put(B_COMMA)
    putAttachedEventIds(attachedEventIds, escaper)
    body?.let {
        put(B_COMMA)
        putBody(body, escaper)
    }
    bodyBase64?.let {
        put(B_COMMA)
        putFieldWithoutEscaping(BODY_BASE_64_FILED, bodyBase64.toByteArray(UTF_8))
    }
    put(B_COMMA)
    putMessageIdField(MESSAGE_ID_FILED, messageId, escaper)
    put(B_CLOSING_CURLY_BRACE)
    flip()
}

fun LwEvent.writeJsonData(buf: ByteBuf, escaper: Escaper): ByteBuf = buf.apply {
    writeByte(OPENING_CURLY_BRACE)
    writeEventIdField(EVENT_ID_FILED, batchId, eventId, escaper)
    writeByte(COMMA)
    batchId?.let { writeEventIdField(BATCH_ID_FILED, null, it, escaper) } ?: run { writeNull(BATCH_ID_FILED) }
    writeByte(COMMA)
    writeField(IS_BATCHED_FILED, isBatched)
    writeByte(COMMA)
    writeField(EVENT_NAME_FILED, event.name, escaper)
    writeByte(COMMA)
    event.type?.let { writeField(EVENT_TYPE_FILED, it, escaper) } ?: run { writeNull(EVENT_TYPE_FILED) }
    writeByte(COMMA)
    event.endTimestamp?.let { writeTimestamp(END_TIMESTAMP_FILED, it) } ?: run { writeNull(END_TIMESTAMP_FILED) }
    writeByte(COMMA)
    writeTimestamp(START_TIMESTAMP_FILED, event.id.startTimestamp)
    writeByte(COMMA)
    event.parentId
        ?.let { writeEventIdField(PARENT_EVENT_ID_FILED, parentBatchId, it, escaper) }
        ?: run { writeNull(PARENT_EVENT_ID_FILED) }
    writeByte(COMMA)
    writeField(SUCCESSFUL_FILED, event.isSuccess)
    writeByte(COMMA)
    writeFieldWithoutEscaping(BOOK_ID_FILED, escaper.escape(bookId, true))
    writeByte(COMMA)
    writeFieldWithoutEscaping(SCOPE_FILED, escaper.escape(scope, true))
    writeByte(COMMA)
    if (attachedMessageIds.isNotEmpty()) {
        writeMessageIdList(ATTACHED_MESSAGE_IDS_FILED, attachedMessageIds, escaper)
    } else {
        writeEmptyList(ATTACHED_MESSAGE_IDS_FILED)
    }
    writeByte(COMMA)
    if (event.content != null && event.content.remaining() > 0) {
        writeBody(BODY_FILED, event.content)
    } else {
        writeEmptyList(BODY_FILED)
    }
    writeByte(CLOSING_CURLY_BRACE)
}

var metadataTime = 0L
var metadataSize = 0L
var messagesTime = 0L
var messagesSize = 0L
var bodyTime = 0L
var bodySize = 0L
var totalTime = 0L
var totalSize = 0L

fun LwEvent.putJsonData(buf: ByteBuffer, escaper: Escaper): ByteBuffer = buf.apply {
    val t0 = System.nanoTime()
    val s0 = position()
    put(B_OPENING_CURLY_BRACE)
    putEventIdField(EVENT_ID_FILED, batchId, eventId, escaper)
    put(B_COMMA)
    batchId?.let { putEventIdField(BATCH_ID_FILED, null, it, escaper) } ?: run { putNull(BATCH_ID_FILED) }
    put(B_COMMA)
    putField(IS_BATCHED_FILED, isBatched)
    put(B_COMMA)
    putField(EVENT_NAME_FILED, event.name, escaper)
    put(B_COMMA)
    event.type?.let { putField(EVENT_TYPE_FILED, it, escaper) } ?: run { putNull(EVENT_TYPE_FILED) }
    put(B_COMMA)
    event.endTimestamp?.let { putTimestamp(END_TIMESTAMP_FILED, it) } ?: run { putNull(END_TIMESTAMP_FILED) }
    put(B_COMMA)
    putTimestamp(START_TIMESTAMP_FILED, event.id.startTimestamp)
    put(B_COMMA)
    event.parentId
        ?.let { putEventIdField(PARENT_EVENT_ID_FILED, parentBatchId, it, escaper) }
        ?: run { putNull(PARENT_EVENT_ID_FILED) }
    put(B_COMMA)
    putField(SUCCESSFUL_FILED, event.isSuccess)
    put(B_COMMA)
    putFieldWithoutEscaping(BOOK_ID_FILED, escaper.escape(bookId, true))
    put(B_COMMA)
    putFieldWithoutEscaping(SCOPE_FILED, escaper.escape(scope, true))
    put(B_COMMA)
    val t1 = System.nanoTime()
    val s1 = position()
    if (attachedMessageIds.isNotEmpty()) {
        putMessageIdList(ATTACHED_MESSAGE_IDS_FILED, attachedMessageIds, escaper)
    } else {
        putEmptyList(ATTACHED_MESSAGE_IDS_FILED)
    }
    put(B_COMMA)
    val t2 = System.nanoTime()
    val s2 = position()
    if (event.content != null && event.content.remaining() > 0) {
        putBody(BODY_FILED, event.content)
    } else {
        putEmptyList(BODY_FILED)
    }
    put(B_CLOSING_CURLY_BRACE)
    val t3 = System.nanoTime()
    val s3 = position()
    metadataTime += t1 - t0
    metadataSize += s1 - s0
    messagesTime += t2 - t1
    messagesSize += s2 - s1
    bodyTime += t3 - t2
    bodySize += s3 - s2
    totalTime += t3 - t0
    totalSize += s3 - s0
    flip()
}

fun LwEvent.appendJsonData(builder: StringBuilder, escaper: Escaper): StringBuilder = builder.apply {
    append('{')
    appendEventIdField(""""eventId"""", batchId, eventId, escaper)
    append(',')
    batchId?.let { appendEventIdField(""""batchId"""", null, it, escaper) } ?: run { appendNull(""""batchId"""") }
    append(',')
    appendField(""""isBatched"""", isBatched)
    append(',')
    appendField(""""eventName"""", event.name, escaper)
    append(',')
    event.type?.let { appendField(""""eventType"""", it, escaper) } ?: run { appendNull(""""eventType"""") }
    append(',')
    event.endTimestamp?.let { appendTimestamp(""""endTimestamp"""", it) } ?: run { appendNull(""""endTimestamp"""") }
    append(',')
    appendTimestamp(""""startTimestamp"""", event.id.startTimestamp)
    append(',')
    event.parentId
        ?.let { appendEventIdField(""""parentEventId"""", parentBatchId, it, escaper) }
        ?: run { appendNull(""""parentEventId"""") }
    append(',')
    appendField(""""successful"""", event.isSuccess)
    append(',')
    appendFieldWithoutEscaping(""""bookId"""", escaper.escapeStr(bookId, true))
    append(',')
    appendFieldWithoutEscaping(""""scope"""", escaper.escapeStr(scope, true))
    append(',')
    if (attachedMessageIds.isNotEmpty()) {
        appendMessageIdList(""""attachedMessageIds"""", attachedMessageIds, escaper)
    } else {
        appendEmptyList(""""attachedMessageIds"""")
    }
//    append(',')
//    if (event.content != null && event.content.remaining() > 0) {
//        appendBody(""""body"""", event.content)
//    } else {
//        appendEmptyList(""""body"""")
//    }
    append('}')
}

private fun ByteBuf.writeMessageId(messageId: StoredMessageId, escaper: Escaper) {
    with(messageId) {
        writeBytes(escaper.escape(cradleEscape(bookId.toString()), true))
        writeByte(COLON)
        writeBytes(escaper.escape(cradleEscape(sessionAlias), true))
        writeByte(COLON)
        writeCharSequence(direction.label, UTF_8)
        writeByte(COLON)
        writeTimestamp(timestamp)
        writeByte(COLON)
        writeCharSequence(sequence.toString(), UTF_8)
    }
}

fun ByteBuffer.putMessageId(messageId: StoredMessageId, escaper: Escaper) {
    with(messageId) {
        put(escaper.escape(bookId.name, true))
        put(B_COLON)
        put(escaper.escape(sessionAlias, true))
        put(B_COLON)
        putDirection(direction)
        put(B_COLON)
        putTimestamp(timestamp)
        put(B_COLON)
        put(sequence.toString().toByteArray(UTF_8))
    }
}

private fun ByteBuffer.putDirection(direction: com.exactpro.cradle.Direction) {
    when(direction) {
        com.exactpro.cradle.Direction.FIRST -> put(FIRST)
        com.exactpro.cradle.Direction.SECOND -> put(SECOND)
        else -> put(direction.label.toByteArray(UTF_8))
    }
}

private fun StringBuilder.appendMessageId(messageId: StoredMessageId, escaper: Escaper) {
    with(messageId) {
        append(escaper.escapeStr(cradleEscape(bookId.toString()), true))
        append(':')
        append(escaper.escapeStr(cradleEscape(sessionAlias), true))
        append(':')
        append(direction.label)
        append(':')
        appendTimestamp(timestamp)
        append(':')
        append(sequence)
    }
}

private fun ByteBuf.writeMessageIdField(name: ByteArray, messageId: StoredMessageId, escaper: Escaper) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(DOUBLE_QUOTE)
    writeMessageId(messageId, escaper)
    writeByte(DOUBLE_QUOTE)
}

private fun ByteBuffer.putMessageIdField(name: ByteArray, messageId: StoredMessageId, escaper: Escaper) {
    put(name)
    put(B_COLON)
    put(B_DOUBLE_QUOTE)
    putMessageId(messageId, escaper)
    put(B_DOUBLE_QUOTE)
}

private fun ByteBuf.writeMessageIdList(name: ByteArray, messageIds: Collection<StoredMessageId>, escaper: Escaper) {
    writeList(name, messageIds) {
        writeByte(DOUBLE_QUOTE)
        writeMessageId(it, escaper)
        writeByte(DOUBLE_QUOTE)
    }
}

private fun ByteBuffer.putMessageIdList(name: ByteArray, messageIds: Collection<StoredMessageId>, escaper: Escaper) {
    putList(name, messageIds) {
        put(B_DOUBLE_QUOTE)
        putMessageId(it, escaper)
        put(B_DOUBLE_QUOTE)
    }
}

private fun StringBuilder.appendMessageIdList(name: String, messageIds: Collection<StoredMessageId>, escaper: Escaper) {
    appendList(name, messageIds) {
        append('"')
        appendMessageId(it, escaper)
        append('"')
    }
}

private fun ByteBuf.writeBody(messages: List<TransportMessageContainer>, escaper: Escaper) {
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
        writeMetadata(parsedMessage, escaper)
        writeByte(COMMA)
        writeFields(parsedMessage)
        writeByte(CLOSING_CURLY_BRACE)
    }
    writeByte(CLOSING_SQUARE_BRACE)
}

private fun ByteBuffer.putBody(messages: List<TransportMessageContainer>, escaper: Escaper) {
    put(BODY_FILED)
    put(B_COLON)
    put(B_OPENING_SQUARE_BRACE)
    messages.forEachIndexed { index, message ->
        val parsedMessage = message.parsedMessage
        if (!parsedMessage.rawBody.isReadable) {
            error("The ${parsedMessage.id} message can't be serialized because raw data is blank")
        }
        if (index != 0) {
            put(B_COMMA)
        }
        put(B_OPENING_CURLY_BRACE)
        putMetadata(parsedMessage, escaper)
        put(B_COMMA)
        putFields(parsedMessage)
        put(B_CLOSING_CURLY_BRACE)
    }
    put(B_CLOSING_SQUARE_BRACE)
}

private fun ByteBuf.writeFields(parsedMessage: ParsedMessage) {
    writeBytes(FIELDS_FILED)
    writeByte(COLON)
    writeBytes(parsedMessage.rawBody.toByteArray())
}

private fun ByteBuffer.putFields(parsedMessage: ParsedMessage) {
    put(FIELDS_FILED)
    put(B_COLON)
    put(parsedMessage.rawBody.toByteArray())
}

private fun ByteBuf.writeMetadata(message: ParsedMessage, escaper: Escaper) {
    writeBytes(METADATA_FILED)
    writeByte(COLON)
    writeByte(OPENING_CURLY_BRACE)

    with(message) {
        if (id.subsequence.isNotEmpty()) {
            writeNumberList(SUBSEQUENCE_FILED, id.subsequence)
            writeByte(COMMA)
        }
        writeField(MESSAGE_TYPE_FILED, type, escaper)
        if (metadata.isNotEmpty()) {
            writeByte(COMMA)
            writeMap(PROPERTIES_FILED, metadata, escaper)
        }
        if (protocol.isNotBlank()) {
            writeByte(COMMA)
            writeFieldWithoutEscaping(PROTOCOL_FILED, escaper.escape(protocol, true))
        }
    }

    writeByte(CLOSING_CURLY_BRACE)
}

private fun ByteBuffer.putMetadata(message: ParsedMessage, escaper: Escaper) {
    put(METADATA_FILED)
    put(B_COLON)
    put(B_OPENING_CURLY_BRACE)

    with(message) {
        if (id.subsequence.isNotEmpty()) {
            putNumberList(SUBSEQUENCE_FILED, id.subsequence)
            put(B_COMMA)
        }
        putField(MESSAGE_TYPE_FILED, type, escaper)
        if (metadata.isNotEmpty()) {
            put(B_COMMA)
            putMap(PROPERTIES_FILED, metadata, escaper)
        }
        if (protocol.isNotBlank()) {
            put(B_COMMA)
            putFieldWithoutEscaping(PROTOCOL_FILED, escaper.escape(protocol, true))
        }
    }

    put(B_CLOSING_CURLY_BRACE)
}

private fun ByteBuf.writeBody(name: ByteArray, value: ByteBuffer) {
    writeBytes(name)
    writeByte(COLON)
    val first = value.get(value.position()).toInt()
    val last = value.get(value.limit() - 1).toInt()
    if (first.let { it == OPENING_SQUARE_BRACE || it == OPENING_CURLY_BRACE }
        && last.let { it == CLOSING_SQUARE_BRACE || it == CLOSING_CURLY_BRACE }) { // TODO: use single write
        writeBytes(
            value.array(),
            value.arrayOffset() + value.position(),
            value.remaining()
        )
    } else {
        writeByte(DOUBLE_QUOTE)
        writeBytes(Base64.getEncoder().encode(value))
        writeByte(DOUBLE_QUOTE)
    }
}

private fun ByteBuffer.putBody(name: ByteArray, value: ByteBuffer) {
    put(name)
    put(B_COLON)
    val first = value.get(value.position()).toInt()
    val last = value.get(value.limit() - 1).toInt()
    if (first.let { it == OPENING_SQUARE_BRACE || it == OPENING_CURLY_BRACE }
        && last.let { it == CLOSING_SQUARE_BRACE || it == CLOSING_CURLY_BRACE }) { // TODO: use single write
        put(value.array(), value.arrayOffset() + value.position(), value.remaining())
    } else {
        put(B_DOUBLE_QUOTE)
        value.mark()
        put(Base64.getEncoder().encode(value))
        value.reset()
        put(B_DOUBLE_QUOTE)
    }
}

private fun StringBuilder.appendBody(name: String, value: ByteBuffer) {
    append(name)
    append(':')
    val first = value.get(value.position()).toInt()
    val last = value.get(value.limit() - 1).toInt()
    if (first.let { it == OPENING_SQUARE_BRACE || it == OPENING_CURLY_BRACE }
        && last.let { it == CLOSING_SQUARE_BRACE || it == CLOSING_CURLY_BRACE }) { // TODO: use single write

        val decoder = UTF_8.newDecoder()
        val charBuffer = decoder.decode(value.asReadOnlyBuffer())
        append(charBuffer)
    } else {
        append('"')
        append(Base64.getEncoder().encode(value))
        append('"')
    }
}

private fun ByteBuf.writeEventId(eventId: StoredTestEventId, escaper: Escaper) {
    writeBytes(escaper.escape(eventId.bookId.name, true))
    writeByte(COLON)
    writeBytes(escaper.escape(eventId.scope, true))
    writeByte(COLON)
    writeTimestamp(eventId.startTimestamp)
    writeByte(COLON)
    writeBytes(escaper.escape(eventId.id, false))
}

private fun ByteBuffer.putEventId(eventId: StoredTestEventId, escaper: Escaper) {
    put(escaper.escape(eventId.bookId.name, true))
    put(B_COLON)
    put(escaper.escape(eventId.scope, true))
    put(B_COLON)
    putTimestamp(eventId.startTimestamp)
    put(B_COLON)
    put(escaper.escape(eventId.id, false))
}

private fun StringBuilder.appendEventId(eventId: StoredTestEventId, escaper: Escaper) {
    append(escaper.escapeStr(eventId.bookId.name, true))
    append(':')
    append(escaper.escapeStr(eventId.scope, true))
    append(':')
    appendTimestamp(eventId.startTimestamp)
    append(':')
    append(escaper.escapeStr(eventId.id, false))
}

private fun ByteBuf.writeTimestamp(timestamp: Instant) {
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

fun ByteBuffer.putTimestamp(timestamp: Instant) {
    timestamp.atZone(ZoneOffset.UTC).apply {
//        putFourDigits(year)
//        putTwoDigits(monthValue)
//        putTwoDigits(dayOfMonth)
//        putTwoDigits(hour)
//        putTwoDigits(minute)
//        putTwoDigits(second)
//        putNineDigits(nano)
        putDigits(year, 1_000)
        putDigits(monthValue, 10)
        putDigits(dayOfMonth, 10)
        putDigits(hour, 10)
        putDigits(minute, 10)
        putDigits(second, 10)
        putDigits(nano, 100_000_000)
    }
}

private fun StringBuilder.appendTimestamp(timestamp: Instant) {
    timestamp.atZone(ZoneOffset.UTC).apply {
        appendFourDigits(year)
        appendTwoDigits(monthValue)
        appendTwoDigits(dayOfMonth)
        appendTwoDigits(hour)
        appendTwoDigits(minute)
        appendTwoDigits(second)
        appendNineDigits(nano)
    }
}

private fun ByteBuf.writeEventIdField(name: ByteArray, batchEventId: StoredTestEventId?, eventId: StoredTestEventId, escaper: Escaper) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(DOUBLE_QUOTE)
    if (batchEventId != null) {
        writeEventId(batchEventId, escaper)
        writeByte(ID_DIVIDER)
    }
    writeEventId(eventId, escaper)
    writeByte(DOUBLE_QUOTE)
}

private fun ByteBuffer.putEventIdField(name: ByteArray, batchEventId: StoredTestEventId?, eventId: StoredTestEventId, escaper: Escaper) {
    put(name)
    put(B_COLON)
    put(B_DOUBLE_QUOTE)
    if (batchEventId != null) {
        putEventId(batchEventId, escaper)
        put(B_ID_DIVIDER)
    }
    putEventId(eventId, escaper)
    put(B_DOUBLE_QUOTE)
}

private fun StringBuilder.appendEventIdField(name: String, batchEventId: StoredTestEventId?, eventId: StoredTestEventId, escaper: Escaper) {
    append(name)
    append(':')
    append('"')
    if (batchEventId != null) {
        appendEventId(batchEventId, escaper)
        append('>')
    }
    appendEventId(eventId, escaper)
    append('"')
}

private fun ByteBuf.writeAttachedEventIds(attachedEventIds: Set<String>, escaper: Escaper) {
    writeBytes(ATTACHED_EVENT_IDS_FILED)
    writeByte(COLON)
    writeByte(OPENING_SQUARE_BRACE)
    attachedEventIds.forEachIndexed { index, eventId ->
        if (index != 0) {
            writeByte(COMMA)
        }
        writeByte(DOUBLE_QUOTE)
        writeBytes(escaper.escape(eventId, false))
        writeByte(DOUBLE_QUOTE)
    }
    writeByte(CLOSING_SQUARE_BRACE)
}

private fun ByteBuffer.putAttachedEventIds(attachedEventIds: Set<String>, escaper: Escaper) {
    put(ATTACHED_EVENT_IDS_FILED)
    put(B_COLON)
    put(B_OPENING_SQUARE_BRACE)
    attachedEventIds.forEachIndexed { index, eventId ->
        if (index != 0) {
            put(B_COMMA)
        }
        put(B_DOUBLE_QUOTE)
        put(escaper.escape(eventId, false))
        put(B_DOUBLE_QUOTE)
    }
    put(B_CLOSING_SQUARE_BRACE)
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

private fun ByteBuffer.putTimestamp(name: ByteArray, timestamp: Instant) {
    put(name)
    put(B_COLON)
    put(B_OPENING_CURLY_BRACE)
    putField(EPOCH_SECOND_FILED, timestamp.epochSecond)
    put(B_COMMA)
    putField(NANO_FILED, timestamp.nano)
    put(B_CLOSING_CURLY_BRACE)
}

private fun StringBuilder.appendTimestamp(name: String, timestamp: Instant) {
    append(name)
    append(':')
    append('{')
    appendField(""""epochSecond"""", timestamp.epochSecond)
    append(',')
    appendField(""""nano"""", timestamp.nano)
    append('}')
}

private fun ByteBuf.writeFourDigits(value: Int) {
    when {
        value < 10 -> writeBytes(ZERO3)
        value < 100 -> writeBytes(ZERO2)
        value < 1000 -> writeByte(ZERO)
    }
    writeCharSequence(value.toString(), UTF_8)
}

private fun ByteBuffer.putFourDigits(value: Int) {
    when {
        value < 10 -> put(ZERO3)
        value < 100 -> put(ZERO2)
        value < 1000 -> put(B_ZERO)
    }
    put(value.toString().toByteArray(UTF_8))
}

private fun StringBuilder.appendFourDigits(value: Int) {
    when {
        value < 10 -> append("000")
        value < 100 -> append("00")
        value < 1000 -> append('0')
    }
    append(value)
}

private fun ByteBuf.writeNineDigits(value: Int) {
    when {
        value < 10 -> writeBytes(ZERO8)
        value < 100 -> writeBytes(ZERO7)
        value < 1000 -> writeBytes(ZERO6)
        value < 10000 -> writeBytes(ZERO5)
        value < 100000 -> writeBytes(ZERO4)
        value < 1000000 -> writeBytes(ZERO3)
        value < 10000000 -> writeBytes(ZERO2)
        value < 100000000 -> writeByte(ZERO)
    }
    writeCharSequence(value.toString(), UTF_8)
}

private fun ByteBuffer.putNineDigits(value: Int) {
    when {
        value < 10 -> put(ZERO8)
        value < 100 -> put(ZERO7)
        value < 1000 -> put(ZERO6)
        value < 10000 -> put(ZERO5)
        value < 100000 -> put(ZERO4)
        value < 1000000 -> put(ZERO3)
        value < 10000000 -> put(ZERO2)
        value < 100000000 -> put(B_ZERO)
    }
    put(value.toString().toByteArray(UTF_8))
}

private fun ByteBuffer.putDigits(value: Int, n: Int) {
    var divisor = n
    var dividend = value
    while (divisor != 0) {
        if (dividend < divisor) {
            put(B_ZERO)
        } else {
            put(('0'.code + dividend / divisor).toByte())
            dividend %= divisor
        }
        divisor /= 10
    }
}

private fun StringBuilder.appendNineDigits(value: Int) {
    when {
        value < 10 -> append("00000000")
        value < 100 -> append("0000000")
        value < 1000 -> append("000000")
        value < 10000 -> append("00000")
        value < 100000 -> append("0000")
        value < 1000000 -> append("000")
        value < 10000000 -> append("00")
        value < 100000000 -> append('0')
    }
    append(value)
}

private fun ByteBuf.writeTwoDigits(value: Int) {
    if (value < 10) {
        writeByte(ZERO)
    }
    writeCharSequence(value.toString(), UTF_8)
}

private fun ByteBuffer.putTwoDigits(value: Int) {
    if (value < 10) {
        put(B_ZERO)
    }
    put(value.toString().toByteArray(UTF_8))
}

private fun StringBuilder.appendTwoDigits(value: Int) {
    if (value < 10) {
        append('0')
    }
    append(value)
}

private fun ByteBuf.writeFieldWithoutEscaping(name: ByteArray, value: ByteArray) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(DOUBLE_QUOTE)
    writeBytes(value)
    writeByte(DOUBLE_QUOTE)
}

private fun ByteBuffer.putFieldWithoutEscaping(name: ByteArray, value: ByteArray) {
    put(name)
    put(B_COLON)
    put(B_DOUBLE_QUOTE)
    put(value)
    put(B_DOUBLE_QUOTE)
}

private fun StringBuilder.appendFieldWithoutEscaping(name: String, value: String) {
    append(name)
    append(':')
    append('"')
    append(value)
    append('"')
}

private fun ByteBuf.writeFieldWithoutEscaping(name: ByteArray, value: String) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(DOUBLE_QUOTE)
    writeCharSequence(value, UTF_8)
    writeByte(DOUBLE_QUOTE)
}

private fun ByteBuf.writeField(name: ByteArray, value: String, escaper: Escaper) = writeFieldWithoutEscaping(name, escaper.escape(value, false))
private fun ByteBuffer.putField(name: ByteArray, value: String, escaper: Escaper) = putFieldWithoutEscaping(name, escaper.escape(value, false))
private fun StringBuilder.appendField(name: String, value: String, escaper: Escaper) = appendFieldWithoutEscaping(name, escaper.escapeStr(value, false))
private fun ByteBuf.writeField(name: ByteArray, value: Boolean) {
    writeBytes(name)
    writeByte(COLON)
    writeBytes(if (value) TRUE else FALSE)
}

private fun ByteBuffer.putField(name: ByteArray, value: Boolean) {
    put(name)
    put(B_COLON)
    put(if (value) TRUE else FALSE)
}

private fun StringBuilder.appendField(name: String, value: Boolean) {
    append(name)
    append(':')
    append(if (value) "true" else "false")
}

private fun ByteBuf.writeField(name: ByteArray, value: Number) {
    writeBytes(name)
    writeByte(COLON)
    writeCharSequence(value.toString(), UTF_8)
}

private fun ByteBuffer.putField(name: ByteArray, value: Number) {
    put(name)
    put(B_COLON)
    put(value.toString().toByteArray(UTF_8))
}

private fun StringBuilder.appendField(name: String, value: Number) {
    append(name)
    append(':')
    append(value)
}

private fun ByteBuf.writeField(name: String, value: String, escaper: Escaper) {
    writeByte(DOUBLE_QUOTE)
    writeBytes(escaper.escape(name, true))
    writeByte(DOUBLE_QUOTE)
    writeByte(COLON)
    writeByte(DOUBLE_QUOTE)
    writeBytes(escaper.escape(value, false))
    writeByte(DOUBLE_QUOTE)
}

private fun ByteBuffer.putField(name: String, value: String, escaper: Escaper) {
    put(B_DOUBLE_QUOTE)
    put(escaper.escape(name, true))
    put(B_DOUBLE_QUOTE)
    put(B_COLON)
    put(B_DOUBLE_QUOTE)
    put(escaper.escape(value, false))
    put(B_DOUBLE_QUOTE)
}

private fun ByteBuf.writeNull(name: ByteArray) {
    writeBytes(name)
    writeByte(COLON)
    writeBytes(NULL)
}

private fun ByteBuffer.putNull(name: ByteArray) {
    put(name)
    put(B_COLON)
    put(NULL)
}

private fun StringBuilder.appendNull(name: String) {
    append(name)
    append(':')
    append("null")
}

private fun ByteBuf.writeEmptyList(name: ByteArray) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(OPENING_SQUARE_BRACE)
    writeByte(CLOSING_SQUARE_BRACE)
}

private fun ByteBuffer.putEmptyList(name: ByteArray) {
    put(name)
    put(B_COLON)
    put(B_OPENING_SQUARE_BRACE)
    put(B_CLOSING_SQUARE_BRACE)
}

private fun StringBuilder.appendEmptyList(name: String) {
    append(name)
    append(":[]")
}

private fun ByteBuf.writeMap(name: ByteArray, value: Map<String, String>, escaper: Escaper) {
    writeBytes(name)
    writeByte(COLON)
    writeByte(OPENING_CURLY_BRACE)
    value.onEachIndexed { index, entry ->
        if (index != 0) {
            writeByte(COMMA)
        }
        writeField(entry.key, entry.value, escaper)
    }
    writeByte(CLOSING_CURLY_BRACE)
}

private fun ByteBuffer.putMap(name: ByteArray, value: Map<String, String>, escaper: Escaper) {
    put(name)
    put(B_COLON)
    put(B_OPENING_CURLY_BRACE)
    value.onEachIndexed { index, entry ->
        if (index != 0) {
            put(B_COMMA)
        }
        putField(entry.key, entry.value, escaper)
    }
    put(B_CLOSING_CURLY_BRACE)
}

private fun ByteBuf.writeNumberList(name: ByteArray, value: Collection<Number>) {
    writeList(name, value) { writeCharSequence(it.toString(), UTF_8) }
}

private fun ByteBuffer.putNumberList(name: ByteArray, value: Collection<Number>) {
    putList(name, value) { put(it.toString().toByteArray(UTF_8)) }
}

private fun StringBuilder.appendNumberList(name: String, value: Collection<Number>) {
    appendList(name, value) { append(it) }
}

private inline fun <T> ByteBuf.writeList(name: ByteArray, values: Collection<T>, writeValue: ByteBuf.(T) -> Unit) {
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

private inline fun <T> ByteBuffer.putList(name: ByteArray, values: Collection<T>, putValue: ByteBuffer.(T) -> Unit) {
    put(name)
    put(B_COLON)
    put(B_OPENING_SQUARE_BRACE)
    val lastIndex = values.size - 1
    values.forEachIndexed { index, value ->
        putValue(value)
        if (lastIndex != index) {
            put(B_COMMA)
        }
    }
    put(B_CLOSING_SQUARE_BRACE)
}

private inline fun <T> StringBuilder.appendList(name: String, values: Collection<T>, writeValue: StringBuilder.(T) -> Unit) {
    append(name)
    append(':')
    append('[')
    val lastIndex = values.size - 1
    values.forEachIndexed { index, value ->
        writeValue(value)
        if (lastIndex != index) {
            append(',')
        }
    }
    append(']')
}

interface ByteBufPool {
    fun acquire(): ByteBuf
    fun release(buf: ByteBuf)
}

object DummyByteBufPool : ByteBufPool {
    override fun acquire(): ByteBuf = Unpooled.buffer()

    override fun release(buf: ByteBuf) {}
}

class UnpooledBufPool(
    private val bufferSize: Int = 1_024 * 2,
) : ByteBufPool, AutoCloseable {
    private val pool = ConcurrentLinkedQueue<ByteBuf>()

    override fun acquire(): ByteBuf = pool.poll()?.clear() ?: Unpooled.buffer(bufferSize)

    override fun release(buf: ByteBuf) {
        if (!pool.offer(buf.clear())) {
            buf.release()
        }
    }

    override fun close() {
        while (pool.isNotEmpty()) {
            acquire().release()
        }
    }
}

interface ByteBufferPool {
    fun acquire(): ByteBuffer
    fun release(buffer: ByteBuffer)
}

object DummyBufferPool : ByteBufferPool {
    override fun acquire(): ByteBuffer = ByteBuffer.allocate(1_024 * 1_024)

    override fun release(buffer: ByteBuffer) {}
}

class HeapBufferPool(
    private val bufferSize: Int = 1_024 * 1_024,
) : ByteBufferPool, AutoCloseable {
    private val pool = ConcurrentLinkedQueue<ByteBuffer>()

    override fun acquire(): ByteBuffer = pool.poll()?.clear() ?: ByteBuffer.allocate(bufferSize)

    override fun release(buffer: ByteBuffer) {
        pool.offer(buffer.clear())
    }

    override fun close() {
        pool.clear()
    }
}