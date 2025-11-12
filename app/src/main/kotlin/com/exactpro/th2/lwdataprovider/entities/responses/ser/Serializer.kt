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

package com.exactpro.th2.lwdataprovider.entities.responses.ser

import com.exactpro.th2.lwdataprovider.Escaper
import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer
import java.util.Base64
import kotlin.math.ceil
import kotlin.math.pow
import kotlin.text.Charsets.UTF_8

interface SerializableChar {
    val int: Int
    val byte: Byte
}

interface SerializableString {
    val bytes: ByteArray
}

enum class SpecialChar(char: Char) : SerializableChar {
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

enum class NumberLength(internal val length: Int) {
    TWO_DIGITS(2),
    FOUR_DIGITS(4),
    NINE_DIGITS(9);
    internal val divisor: Int = 10.0.pow(length - 1).toInt()

}

@DslMarker
annotation class SerializerDsl

@SerializerDsl
sealed interface Serializer<S : Serializer<S>> {
    fun serialize(block: S.() -> Unit)

    fun obj(block: S.() -> Unit): S
    fun arr(block: S.() -> Unit): S

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
    fun base64Str(value: ByteBuffer): S
    fun base64Str(value: ByteArray): S

    fun filed(name: SerializableString, value: S.() -> Unit): S
    fun filedStr(name: SerializableString, value: S.() -> Unit): S
    fun filedBool(name: SerializableString, value: Boolean): S
}

fun serialize(buffer: ByteBuffer, escaper: Escaper, block: (Serializer<*>) -> Unit): ByteBuffer = buffer.apply {
    ByteBufferSerializer(this, escaper).serialize(block)
}

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
            put(JsonChar.OPENING_CURLY_BRACE.byte)
            block()
            put(JsonChar.CLOSING_CURLY_BRACE.byte)
        }
    }

    override fun arr(block: ByteBufferSerializer.() -> Unit) = this.also {
        with(buffer) {
            put(JsonChar.OPENING_SQUARE_BRACE.byte)
            it.block()
            put(JsonChar.CLOSING_SQUARE_BRACE.byte)
        }
    }

    override fun valueStr(value: ByteBufferSerializer.() -> Unit) = this.also {
        with(buffer) {
            put(JsonChar.DOUBLE_QUOTE.byte)
            value()
            put(JsonChar.DOUBLE_QUOTE.byte)
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
        this.buffer.put(buffer)
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

    override fun base64Str(value: ByteBuffer) = this.also {
        buffer.put(Base64.getEncoder().encode(value))
    }

    override fun base64Str(value: ByteArray) = this.also {
        buffer.put(Base64.getEncoder().encode(value))
    }

    override fun filed(name: SerializableString, value: ByteBufferSerializer.() -> Unit) = this.also {
        with(buffer) {
            put(name.bytes)
            put(JsonChar.COLON.byte)
            value()
        }
    }

    override fun filedStr(name: SerializableString, value: ByteBufferSerializer.() -> Unit) = this.also {
        with(buffer) {
            put(name.bytes)
            put(JsonChar.COLON.byte)
            put(JsonChar.DOUBLE_QUOTE.byte)
            value()
            put(JsonChar.DOUBLE_QUOTE.byte)
        }
    }

    override fun filedBool(name: SerializableString, value: Boolean) = this.also {
        with(buffer) {
            put(name.bytes)
            put(JsonChar.COLON.byte)
            put(if (value) JsonString.TRUE.bytes else JsonString.FALSE.bytes)
        }
    }

    companion object {
        private fun ByteBuffer.putNumAsStr(value: Int, length: NumberLength) {
            var divisor = length.divisor
            var dividend = value
            while (divisor != 0) {
                if (dividend < divisor) {
                    put(SpecialChar.ZERO.byte)
                } else {
                    put((SpecialChar.ZERO.int + dividend / divisor).toByte())
                    dividend %= divisor
                }
                divisor /= 10
            }
        }
    }
}

fun serialize(buf: ByteBuf, escaper: Escaper, block: (Serializer<*>) -> Unit): ByteBuf = buf.apply {
    ByteBufSerializer(this, escaper).serialize(block)
}

@Suppress("UNCHECKED_CAST")
private class ByteBufSerializer(
    private val buf: ByteBuf,
    private val escaper: Escaper
) : Serializer<ByteBufSerializer> {

    override fun serialize(block: ByteBufSerializer.() -> Unit) = block()

    override fun obj(block: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeByte(JsonChar.OPENING_CURLY_BRACE.int)
            block()
            writeByte(JsonChar.CLOSING_CURLY_BRACE.int)
        }
    }

    override fun arr(block: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeByte(JsonChar.OPENING_SQUARE_BRACE.int)
            it.block()
            writeByte(JsonChar.CLOSING_SQUARE_BRACE.int)
        }
    }

    override fun valueStr(value: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeByte(JsonChar.DOUBLE_QUOTE.int)
            value()
            writeByte(JsonChar.DOUBLE_QUOTE.int)
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
        buf.writeBytes(buffer)
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

    override fun base64Str(value: ByteBuffer) = this.also {
        buf.writeBytes(Base64.getEncoder().encode(value))
    }

    override fun base64Str(value: ByteArray): ByteBufSerializer = this.also {
        buf.writeBytes(Base64.getEncoder().encode(value))
    }

    override fun filed(name: SerializableString, value: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeBytes(name.bytes)
            writeByte(JsonChar.COLON.int)
            value()
        }
    }

    override fun filedStr(name: SerializableString, value: ByteBufSerializer.() -> Unit) = this.also {
        with(buf) {
            writeBytes(name.bytes)
            writeByte(JsonChar.COLON.int)
            writeByte(JsonChar.DOUBLE_QUOTE.int)
            value()
            writeByte(JsonChar.DOUBLE_QUOTE.int)
        }
    }

    override fun filedBool(name: SerializableString, value: Boolean) = this.also {
        with(buf) {
            writeBytes(name.bytes)
            writeByte(JsonChar.COLON.int)
            writeBytes(if (value) JsonString.TRUE.bytes else JsonString.FALSE.bytes)
        }
    }

    companion object {
        private fun ByteBuf.putNumAsStr(value: Int, length: NumberLength) {
            var divisor = length.divisor
            var dividend = value
            while (divisor != 0) {
                if (dividend < divisor) {
                    writeByte(SpecialChar.ZERO.int)
                } else {
                    writeByte((SpecialChar.ZERO.int + dividend / divisor))
                    dividend %= divisor
                }
                divisor /= 10
            }
        }
    }
}

/**
 * Calculates approximate size of serialised data multiply by factor
 */
fun calculateSize(block: (Serializer<*>) -> Unit): Int = SizeSerializer().apply {
    serialize(block)
}.size

private class SizeSerializer(
    private val factor: Double = 1.3
): Serializer<SizeSerializer> {
    private var _size: Int = 0

    val size: Int
        get() = if (factor == 1.0) _size else (_size * factor).toInt()

    init {
        require(factor >= 1.0) {
            "factor '${factor}' can't be less than 1.0"
        }
    }

    override fun serialize(block: SizeSerializer.() -> Unit) = block()

    override fun obj(block: SizeSerializer.() -> Unit) = this.also {
        _size += 2
        block()
    }

    override fun arr(block: SizeSerializer.() -> Unit) = this.also {
        _size += 2
        block()
    }

    override fun valueStr(value: SizeSerializer.() -> Unit) = this.also {
        _size += 2
        value()
    }

    override fun str(value: SerializableString) = this.also {
        _size += value.bytes.size
    }

    override fun str(value: String) = this.also {
        _size += value.length // size in characters != size in bytes (UTF_8)
    }

    override fun char(value: SerializableChar) = this.also {
        _size += 1
    }

    override fun bytes(buffer: ByteBuffer) = this.also {
        _size += buffer.remaining()
    }

    override fun bytes(buf: ByteBuf) = this.also {
        _size += buf.readableBytes()
    }

    override fun escapeStr(
        value: String,
        remember: Boolean
    ) = this.also {
        _size += value.length // size in characters != size in bytes (UTF_8)
    }

    override fun numAsStr(
        value: Int,
        length: NumberLength
    ) = this.also {
        _size += length.length
    }

    override fun numAsStr(value: Int) = this.also {
        _size += 11 // used max value to improve performance
    }

    override fun numAsStr(value: Long) = this.also {
        _size += 20 // used max value to improve performance
    }

    override fun base64Str(value: ByteBuffer) = this.also {
        _size += (4 * ceil((value.remaining() / 3).toDouble())).toInt()
    }

    override fun base64Str(value: ByteArray) = this.also {
        _size += (4 * ceil((value.size / 3).toDouble())).toInt()
    }

    override fun filed(
        name: SerializableString,
        value: SizeSerializer.() -> Unit
    ) = this.also {
        _size += name.bytes.size + 1
        value()
    }

    override fun filedStr(
        name: SerializableString,
        value: SizeSerializer.() -> Unit
    ) = this.also {
        _size += name.bytes.size + 3
        value()
    }

    override fun filedBool(
        name: SerializableString,
        value: Boolean
    ) = this.also {
        _size += name.bytes.size + 1 + (if (value) JsonString.TRUE else JsonString.FALSE).bytes.size
    }
}