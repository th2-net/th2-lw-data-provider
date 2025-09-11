/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.lwdataprovider.Escaper
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.CLOSING_CURLY_BRACE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.CLOSING_SQUARE_BRACE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.COLON
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.COMMA
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.DOUBLE_QUOTE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.OPENING_CURLY_BRACE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonChar.OPENING_SQUARE_BRACE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonString.FALSE
import com.exactpro.th2.lwdataprovider.entities.responses.JsonString.NULL
import com.exactpro.th2.lwdataprovider.entities.responses.JsonString.TRUE
import com.exactpro.th2.lwdataprovider.entities.responses.SpecialChar.GREATER_THAN
import com.exactpro.th2.lwdataprovider.entities.responses.SpecialChar.ONE
import com.exactpro.th2.lwdataprovider.entities.responses.SpecialChar.TWO
import com.exactpro.th2.lwdataprovider.entities.responses.SpecialChar.ZERO
import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer
import kotlin.text.Charsets.UTF_8
import kotlin.text.toByteArray

interface SerializableChar {
    val int: Int
    val byte: Byte
}

interface SerializableString {
    val bytes: ByteArray
}

private enum class SpecialChar(char: Char) : SerializableChar {
    GREATER_THAN('>'),
    ZERO('0'),
    ONE('1'),
    TWO('2');

    override val int = char.code
    override val byte = int.toByte()
}

enum class SpecialString(str: String) : SerializableString {
    IN("IN"),
    OUT("OUT");

    override val bytes = str.toByteArray(UTF_8)
}

enum class JsonChar(char: Char) : SerializableChar {
    OPENING_CURLY_BRACE('{'),
    CLOSING_CURLY_BRACE('}'),
    OPENING_SQUARE_BRACE('['),
    CLOSING_SQUARE_BRACE(']'),
    COMMA(','),
    COLON(':'),
    DOUBLE_QUOTE('"');

    override val int = char.code
    override val byte = int.toByte()
}

enum class JsonString(str: String) : SerializableString {
    NULL("null"),
    TRUE("true"),
    FALSE("false");

    override val bytes = str.toByteArray(UTF_8)
}

enum class EntityField(
    srt: String
) : SerializableString {
    BOOK_ID(""""bookId""""),
    SCOPE(""""scope""""),
    DIRECTION(""""direction""""),
    SUBSEQUENCE(""""subsequence""""),
    PROPERTIES(""""properties""""),
    PROTOCOL(""""protocol""""),
    SESSION_ID(""""sessionId""""),
    MESSAGE_ID(""""messageId""""),
    EVENT_ID(""""eventId""""),
    BATCH_ID(""""batchId""""),
    PARENT_EVENT_ID(""""parentEventId""""),
    METADATA(""""metadata""""),
    IS_BATCHED(""""isBatched""""),
    SUCCESSFUL(""""successful""""),
    MESSAGE_TYPE(""""messageType""""),
    EVENT_NAME(""""eventName""""),
    EVENT_TYPE(""""eventType""""),
    TIMESTAMP(""""timestamp""""),
    END_TIMESTAMP(""""endTimestamp""""),
    START_TIMESTAMP(""""startTimestamp""""),
    EPOCH_SECOND(""""epochSecond""""),
    NANO(""""nano""""),
    ATTACHED_MESSAGE_IDS(""""attachedMessageIds""""),
    ATTACHED_EVENT_IDS(""""attachedEventIds""""),
    FIELDS(""""fields""""),
    BODY(""""body""""),
    BODY_BASE_64(""""bodyBase64"""");

    override val bytes = srt.toByteArray(UTF_8)
}

enum class NumberLength(internal val divisor: Int) {
    TWO(10),
    FOUR(1_000),
    NINE(100_000_000),
}

@DslMarker
annotation class SerializerDsl

@SerializerDsl
sealed interface Serializer<S : Serializer<S>> {
    fun serialize(block: S.() -> Unit)
    fun obj(block: S.() -> Unit): S
    fun arr(block: S.() -> Unit): S

    fun colon(): S
    fun comma(): S
    fun nul(): S

    fun greaterThan(): S
    fun one(): S
    fun two(): S

    fun valueStr(value: S.() -> Unit): S

    fun str(value: SerializableString): S
    fun str(value: String): S
    fun char(value: SerializableChar): S
    fun bytes(buffer: ByteBuffer): S
    fun bytes(buf: ByteBuf): S

    fun escapeStr(value: String, remember: Boolean = false): S
    fun numAsStr(value: Int, length: NumberLength): S
    fun numAsStr(value: Int): S
    fun numAsStr(value: Long): S

    fun filed(name: SerializableString, value: S.() -> Unit): S
    fun filedStr(name: SerializableString, value: S.() -> Unit): S
    fun filedBool(name: SerializableString, value: Boolean): S
}

fun create(buffer: ByteBuffer, escaper: Escaper): Serializer<*> = ByteBufferSerializer(buffer, escaper)

@Suppress("UNCHECKED_CAST")
private class ByteBufferSerializer(
    private val buffer: ByteBuffer,
    private val escaper: Escaper
) : Serializer<ByteBufferSerializer> {

    override fun serialize(block: ByteBufferSerializer.() -> Unit) {
        block()
        buffer.flip()
    }

    override fun obj(block: ByteBufferSerializer.() -> Unit) = this.also {
        with(buffer) {
            put(OPENING_CURLY_BRACE.byte)
            it.block()
            put(CLOSING_CURLY_BRACE.byte)
        }
    }

    override fun arr(block: ByteBufferSerializer.() -> Unit) = this.also {
        with(buffer) {
            put(OPENING_SQUARE_BRACE.byte)
            it.block()
            put(CLOSING_SQUARE_BRACE.byte)
        }
    }

    override fun colon() = this.also {
        buffer.put(COLON.byte)
    }

    override fun comma() = this.also {
        buffer.put(COMMA.byte)
    }

    override fun nul() = this.also {
        buffer.put(NULL.bytes)
    }

    override fun greaterThan() = this.also {
        buffer.put(GREATER_THAN.byte)
    }

    override fun one() = this.also {
        buffer.put(ONE.byte)
    }

    override fun two() = this.also {
        buffer.put(TWO.byte)
    }

    override fun valueStr(value: ByteBufferSerializer.() -> Unit) = this.also {
        with(buffer) {
            put(DOUBLE_QUOTE.byte)
            value()
            put(DOUBLE_QUOTE.byte)
        }
    }

    override fun str(value: SerializableString) = this.also {
        buffer.put(value.bytes)
    }

    override fun str(value: String) = this.also {
        buffer.put(value.toByteArray(UTF_8))
    }

    override fun char(value: SerializableChar) = this.also {
        buffer.put(value.byte)
    }

    override fun bytes(buffer: ByteBuffer) = this.also {
        buffer.mark()
        this.buffer.put(buffer)
        buffer.reset()
    }

    override fun bytes(buf: ByteBuf) = this.also {
        buf.markReaderIndex()
        buffer.put(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes())
        buf.resetReaderIndex()
    }

    override fun escapeStr(value: String, remember: Boolean) = this.also {
        buffer.put(escaper.escape(value, remember))
    }

    override fun numAsStr(value: Int, length: NumberLength) = this.also {
        buffer.putNumAsStr(value, length)
    }

    override fun numAsStr(value: Int) = this.also {
        buffer.put(value.toString().toByteArray(UTF_8))
    }

    override fun numAsStr(value: Long) = this.also {
        buffer.put(value.toString().toByteArray(UTF_8))
    }

    override fun filed(name: SerializableString, value: ByteBufferSerializer.() -> Unit) = this.also {
        with(buffer) {
            put(name.bytes)
            put(COLON.byte)
            value()
        }
    }

    override fun filedStr(name: SerializableString, value: ByteBufferSerializer.() -> Unit) = this.also {
        with(buffer) {
            put(name.bytes)
            put(COLON.byte)
            put(DOUBLE_QUOTE.byte)
            value()
            put(DOUBLE_QUOTE.byte)
        }
    }

    override fun filedBool(name: SerializableString, value: Boolean) = this.also {
        with(buffer) {
            put(name.bytes)
            put(COLON.byte)
            put(if (value) TRUE.bytes else FALSE.bytes)
        }
    }

    companion object {
        private fun ByteBuffer.putNumAsStr(value: Int, length: NumberLength) {
            var divisor = length.divisor
            var dividend = value
            while (divisor != 0) {
                if (dividend < divisor) {
                    put(ZERO.byte)
                } else {
                    put((ZERO.int + dividend / divisor).toByte())
                    dividend %= divisor
                }
                divisor /= 10
            }
        }
    }
}

fun create(buf: ByteBuf, escaper: Escaper): Serializer<*> = ByteBufSerializer(buf, escaper)

@Suppress("UNCHECKED_CAST")
private class ByteBufSerializer(
    private val buf: ByteBuf,
    private val escaper: Escaper
) : Serializer<ByteBufSerializer> {

    override fun serialize(block: ByteBufSerializer.() -> Unit) {
        this.block()
    }

    override fun obj(block: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeByte(OPENING_CURLY_BRACE.int)
            it.block()
            writeByte(CLOSING_CURLY_BRACE.int)
        }
    }

    override fun arr(block: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeByte(OPENING_SQUARE_BRACE.int)
            it.block()
            writeByte(CLOSING_SQUARE_BRACE.int)
        }
    }

    override fun colon() = this.also {
        buf.writeByte(COLON.int)
    }

    override fun comma() = this.also {
        buf.writeByte(COMMA.int)
    }

    override fun nul() = this.also {
        buf.writeBytes(NULL.bytes)
    }

    override fun greaterThan() = this.also {
        buf.writeByte(GREATER_THAN.int)
    }

    override fun one() = this.also {
        buf.writeByte(ONE.int)
    }

    override fun two() = this.also {
        buf.writeByte(TWO.int)
    }

    override fun valueStr(value: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeByte(DOUBLE_QUOTE.int)
            value()
            writeByte(DOUBLE_QUOTE.int)
        }
    }

    override fun str(value: SerializableString) = this.also {
        buf.writeBytes(value.bytes)
    }

    override fun str(value: String) = this.also {
        buf.writeCharSequence(value, UTF_8)
    }

    override fun char(value: SerializableChar) = this.also {
        buf.writeByte(value.int)
    }

    override fun bytes(buffer: ByteBuffer) = this.also {
        buffer.mark()
        buf.writeBytes(
            buffer.array(),
            buffer.arrayOffset() + buffer.position(),
            buffer.remaining()
        )
        buffer.reset()
    }

    override fun bytes(buf: ByteBuf) = this.also {
        buf.markReaderIndex()
        this.buf.writeBytes(buf)
        buf.resetReaderIndex()
    }

    override fun escapeStr(value: String, remember: Boolean) = this.also {
        buf.writeBytes(escaper.escape(value, remember))
    }

    override fun numAsStr(value: Int, length: NumberLength) = this.also {
        buf.putNumAsStr(value, length)
    }

    override fun numAsStr(value: Int) = this.also {
        buf.writeCharSequence(value.toString(), UTF_8)
    }

    override fun numAsStr(value: Long) = this.also {
        buf.writeCharSequence(value.toString(), UTF_8)
    }

    override fun filed(name: SerializableString, value: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeBytes(name.bytes)
            writeByte(COLON.int)
            value()
        }
    }

    override fun filedStr(name: SerializableString, value: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeBytes(name.bytes)
            writeByte(COLON.int)
            writeByte(DOUBLE_QUOTE.int)
            value()
            writeByte(DOUBLE_QUOTE.int)
        }
    }

    override fun filedBool(name: SerializableString, value: Boolean) = this.also {
        with(buf) {
            writeBytes(name.bytes)
            writeByte(COLON.int)
            writeBytes(if (value) TRUE.bytes else FALSE.bytes)
        }
    }

    companion object {
        private fun ByteBuf.putNumAsStr(value: Int, length: NumberLength) {
            var divisor = length.divisor
            var dividend = value
            while (divisor != 0) {
                if (dividend < divisor) {
                    writeByte(ZERO.int)
                } else {
                    writeByte((ZERO.int + dividend / divisor))
                    dividend %= divisor
                }
                divisor /= 10
            }
        }
    }
}