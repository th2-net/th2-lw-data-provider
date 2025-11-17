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
import kotlin.text.Charsets.UTF_8

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

fun serialize(buf: ByteBuf, escaper: Escaper, block: (Serializer<*>) -> Unit): ByteBuf = buf.apply {
    ByteBufSerializer(this, escaper).serialize(block)
}