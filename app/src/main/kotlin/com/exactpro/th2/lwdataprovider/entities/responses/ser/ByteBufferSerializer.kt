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
        buffer.put(value.toByteArray(Charsets.UTF_8))
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
        buffer.put(value.toString().toByteArray(Charsets.UTF_8))
    }

    override fun numAsStr(value: Long) = this.also {
        buffer.put(value.toString().toByteArray(Charsets.UTF_8))
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

fun serialize(buffer: ByteBuffer, escaper: Escaper, block: (Serializer<*>) -> Unit): ByteBuffer = buffer.apply {
    ByteBufferSerializer(this, escaper).serialize(block)
}