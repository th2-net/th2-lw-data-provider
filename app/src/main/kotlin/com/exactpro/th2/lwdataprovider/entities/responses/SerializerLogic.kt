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

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.entities.internal.Direction.IN
import com.exactpro.th2.lwdataprovider.entities.internal.Direction.OUT
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.ATTACHED_EVENT_IDS
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.ATTACHED_MESSAGE_IDS
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.BATCH_ID
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.BODY
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.BODY_BASE_64
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.BOOK_ID
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.DIRECTION
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.END_TIMESTAMP
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.EPOCH_SECOND
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.EVENT_ID
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.EVENT_NAME
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.EVENT_TYPE
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.FIELDS
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.IS_BATCHED
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.MESSAGE_ID
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.MESSAGE_TYPE
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.METADATA
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.NANO
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.PARENT_EVENT_ID
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.PROPERTIES
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.PROTOCOL
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.SCOPE
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.SESSION_ID
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.START_TIMESTAMP
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.SUBSEQUENCE
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.SUCCESSFUL
import com.exactpro.th2.lwdataprovider.entities.responses.EntityField.TIMESTAMP
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.CLOSING_CURLY_BRACE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.CLOSING_SQUARE_BRACE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.COLON
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.COMMA
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.OPENING_CURLY_BRACE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.OPENING_SQUARE_BRACE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonString.NULL
import com.exactpro.th2.lwdataprovider.entities.responses.NumberLength.FOUR_DIGITS
import com.exactpro.th2.lwdataprovider.entities.responses.NumberLength.NINE_DIGITS
import com.exactpro.th2.lwdataprovider.entities.responses.NumberLength.TWO_DIGITS
import com.exactpro.th2.lwdataprovider.entities.responses.SpecialChar.GREATER_THAN
import com.exactpro.th2.lwdataprovider.entities.responses.SpecialChar.ONE
import com.exactpro.th2.lwdataprovider.entities.responses.SpecialChar.TWO
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.lang.AutoCloseable
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentLinkedQueue

private val SPACE = ' '.code.toByte()
private val TAB = '\t'.code.toByte()

fun ProviderMessage53Transport.serializeJsonData(serializer: Serializer<*>): Unit = with(serializer) {
    obj {
        filed(TIMESTAMP) { timestampObj(timestamp) }.char(COMMA)
        direction?.let {
            filedStr(DIRECTION) {
                when (direction) {
                    IN -> str(SpecialString.IN)
                    OUT -> str(SpecialString.OUT)
                }
            }.char(COMMA)
        }
        filedStr(SESSION_ID) { escapeStr(sessionId, true) }.char(COMMA)
        filed(ATTACHED_EVENT_IDS) {
            arr {
                attachedEventIds.iterate({ char(COMMA) }) {
                    valueStr { escapeStr(it, false) }
                }
            }
        }.char(COMMA)
        body?.let {
            filed(BODY) { body(body) }.char(COMMA)
        }
        bodyBase64?.let {
            filedStr(BODY_BASE_64) { str(bodyBase64) }.char(COMMA)
        }
        filedStr(MESSAGE_ID) { messageId(messageId) }
    }
}

fun LwEvent.serializeJsonData(serializer: Serializer<*>): Unit = with(serializer) {
    obj {
        filedStr(EVENT_ID) { compositeEventId(batchId, eventId) }.char(COMMA)
        filed(BATCH_ID) {
            batchId?.let { valueStr { simpleEventId(it) } }
                ?: run { str(NULL) }
        }.char(COMMA)
        filedBool(IS_BATCHED, isBatched).char(COMMA)
        filedStr(EVENT_NAME) { escapeStr(event.name, false) }.char(COMMA)
        filed(EVENT_TYPE) {
            event.type?.let { valueStr { escapeStr(it, false) } }
                ?: run { str(NULL) }
        }.char(COMMA)
        filed(END_TIMESTAMP) {
            event.endTimestamp?.let { timestampObj(it) }
                ?: run { str(NULL) }
        }.char(COMMA)
        filed(START_TIMESTAMP) { timestampObj(event.id.startTimestamp) }.char(COMMA)
        filed(PARENT_EVENT_ID) {
            event.parentId?.let { valueStr { compositeEventId(parentBatchId, it) } }
                ?: run { str(NULL) }
        }.char(COMMA)
        filedBool(SUCCESSFUL, event.isSuccess).char(COMMA)
        filedStr(BOOK_ID) { escapeStr(event.id.bookId.name, true) }.char(COMMA)
        filedStr(SCOPE) { escapeStr(event.id.scope, true) }.char(COMMA)
        filed(ATTACHED_MESSAGE_IDS) {
            arr {
                attachedMessageIds.iterate({ char(COMMA) }) {
                    valueStr { messageId(it) }
                }
            }
        }.char(COMMA)
        filed(BODY) {
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
        escapeStr(bookId.name, true).char(COLON)
        escapeStr(sessionAlias, true).char(COLON)
        when(direction) {
            Direction.FIRST -> char(ONE)
            Direction.SECOND -> char(TWO)
            else -> escapeStr(direction.label, true)
        }.char(COLON)
        timestampStr(timestamp).char(COLON)
        numAsStr(sequence)
    }
}

private fun Serializer<*>.body(messages: List<TransportMessageContainer>) {
    arr {
        messages.iterate({ char(COMMA) }) {
            val parsedMessage = it.parsedMessage
            if (!parsedMessage.rawBody.isReadable) {
                error("The ${parsedMessage.id} message can't be serialized because raw data is blank")
            }
            obj {
                filed(METADATA) {
                    metadata(parsedMessage)
                }.char(COMMA)
                filed(FIELDS) {
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
                filed(SUBSEQUENCE) {
                    arr {
                        id.subsequence.iterate({ char(COMMA) }) {
                            numAsStr(it)
                        }
                    }
                }.char(COMMA)
            }
            filedStr(MESSAGE_TYPE) { escapeStr(type, false) }
            if (metadata.isNotEmpty()) {
                char(COMMA)
                filed(PROPERTIES) {
                    obj {
                        metadata.iterate({ char(COMMA) }) {
                            valueStr { escapeStr(it.key) }
                                .char(COLON)
                                .valueStr { escapeStr(it.value) }
                        }
                    }
                }
            }
            if (protocol.isNotBlank()) {
                char(COMMA)
                filedStr(PROTOCOL) { escapeStr(protocol, true) }
            }
        }
    }
}

private fun Serializer<*>.body(value: ByteBuffer) {
    val startPos = value.position()
    val endPos = value.limit() - 1

    fun Iterable<Int>.findValue(): Byte = asSequence().map(value::get).filter { it != SPACE && it != TAB }.first()
    fun Byte.isOpeningBrace() = this == OPENING_SQUARE_BRACE.byte || this == OPENING_CURLY_BRACE.byte
    fun Byte.isClosingBrace() = this == CLOSING_SQUARE_BRACE.byte || this == CLOSING_CURLY_BRACE.byte

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
    escapeStr(eventId.bookId.name, true).char(COLON)
    escapeStr(eventId.scope, true).char(COLON)
    timestampStr(eventId.startTimestamp).char(COLON)
    escapeStr(eventId.id)
}

private fun Serializer<*>.timestampStr(timestamp: Instant): Serializer<*> = this.also {
    timestamp.atZone(ZoneOffset.UTC).apply {
        numAsStr(year, FOUR_DIGITS)
        numAsStr(monthValue, TWO_DIGITS)
        numAsStr(dayOfMonth, TWO_DIGITS)
        numAsStr(hour, TWO_DIGITS)
        numAsStr(minute, TWO_DIGITS)
        numAsStr(second, TWO_DIGITS)
        numAsStr(nano, NINE_DIGITS)
    }
}

private fun Serializer<*>.timestampObj(timestamp: Instant) {
    obj {
        filed(EPOCH_SECOND) { numAsStr(timestamp.epochSecond) }.char(COMMA)
        filed(NANO) { numAsStr(timestamp.nano) }
    }
}

private fun Serializer<*>.compositeEventId(batchEventId: StoredTestEventId?, eventId: StoredTestEventId) {
    if (batchEventId != null) {
        simpleEventId(batchEventId).char(GREATER_THAN)
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

interface ByteBufPool {
    fun acquire(): ByteBuf
    fun release(buf: ByteBuf)
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