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
import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.Base64

@Suppress("UNCHECKED_CAST")
private class OutputStreamSerializer(
    private val output: OutputStream,
    private val escaper: Escaper
) : Serializer<OutputStreamSerializer> {

    private val base64Output = Base64.getEncoder().wrap(output)

    override fun serialize(block: OutputStreamSerializer.() -> Unit) {
        block()
    }

    override fun obj(block: OutputStreamSerializer.() -> Unit) = this.also {
        with(output) {
            write(JsonChar.OPENING_CURLY_BRACE.int)
            block()
            write(JsonChar.CLOSING_CURLY_BRACE.int)
        }
    }

    override fun arr(block: OutputStreamSerializer.() -> Unit) = this.also {
        with(output) {
            write(JsonChar.OPENING_SQUARE_BRACE.int)
            it.block()
            write(JsonChar.CLOSING_SQUARE_BRACE.int)
        }
    }

    override fun valueStr(value: OutputStreamSerializer.() -> Unit) = this.also {
        with(output) {
            write(JsonChar.DOUBLE_QUOTE.int)
            value()
            write(JsonChar.DOUBLE_QUOTE.int)
        }
    }

    override fun str(value: SerializableString) = this.also {
        output.write(value.bytes)
    }

    override fun str(value: String) = this.also {
        output.write(value.toByteArray(Charsets.UTF_8))
    }

    override fun char(value: SerializableChar) = this.also {
        output.write(value.int)
    }

    override fun bytes(buffer: ByteBuffer) = this.also {
        this.output.write(buffer.array())
    }

    override fun bytes(buf: ByteBuf) = this.also {
        buf.markReaderIndex()
        output.write(buf.array(), buf.arrayOffset() + buf.readerIndex(), buf.readableBytes())
        buf.resetReaderIndex()
    }

    override fun escapeStr(value: String, remember: Boolean) = this.also {
        output.write(escaper.escape(value, remember))
    }

    override fun numAsStr(value: Int, length: NumberLength) = this.also {
        output.putNumAsStr(value, length)
    }

    override fun numAsStr(value: Int) = this.also {
        output.write(value.toString().toByteArray(Charsets.UTF_8))
    }

    override fun numAsStr(value: Long) = this.also {
        output.write(value.toString().toByteArray(Charsets.UTF_8))
    }

    override fun base64Str(value: ByteBuffer) = this.also {
        val position = value.position()
        try {
            if (value.hasArray()) {
                val offset = value.arrayOffset() + value.position()
                val length = value.remaining()
                base64Output.write(value.array(), offset, length)
            } else {
                val temp = ByteArray(value.remaining())
                value.get(temp)
                base64Output.write(temp)
            }
        } finally {
            value.position(position)
        }
    }

    override fun base64Str(value: ByteArray) = this.also {
        base64Output.write(value)
    }

    override fun filed(name: SerializableString, value: OutputStreamSerializer.() -> Unit) = this.also {
        with(output) {
            write(name.bytes)
            write(JsonChar.COLON.int)
            value()
        }
    }

    override fun filedStr(name: SerializableString, value: OutputStreamSerializer.() -> Unit) = this.also {
        with(output) {
            write(name.bytes)
            write(JsonChar.COLON.int)
            write(JsonChar.DOUBLE_QUOTE.int)
            value()
            write(JsonChar.DOUBLE_QUOTE.int)
        }
    }

    override fun filedBool(name: SerializableString, value: Boolean) = this.also {
        with(output) {
            write(name.bytes)
            write(JsonChar.COLON.int)
            write(if (value) JsonString.TRUE.bytes else JsonString.FALSE.bytes)
        }
    }

    companion object {
        private fun OutputStream.putNumAsStr(value: Int, length: NumberLength) {
            var divisor = length.divisor
            var dividend = value
            while (divisor != 0) {
                if (dividend < divisor) {
                    write(SpecialChar.ZERO.int)
                } else {
                    write(SpecialChar.ZERO.int + dividend / divisor)
                    dividend %= divisor
                }
                divisor /= 10
            }
        }
    }
}

fun serialize(output: OutputStream, escaper: Escaper, block: (Serializer<*>) -> Unit): OutputStream = output.apply {
    OutputStreamSerializer(this, escaper).serialize(block)
}