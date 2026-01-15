/*
 * Copyright 2025-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider

import kotlin.text.Charsets.UTF_8
import kotlin.text.toByteArray

interface Escaper {
    fun escape(value: String, hold: Boolean): ByteArray
}

object DummyEscaper : Escaper {
    override fun escape(value: String, hold: Boolean): ByteArray = jsonEscape(value).toByteArray(UTF_8)
}

class MapEscaper(
    private val holder: MutableMap<String, ByteArray> = mutableMapOf(),
) : Escaper, AutoCloseable {
    override fun escape(value: String, hold: Boolean): ByteArray {
        if (hold) {
            return holder.computeIfAbsent(value) {
                jsonEscape(value).toByteArray(UTF_8)
            }
        }
        return jsonEscape(value).toByteArray(UTF_8)
    }

    override fun close() {
        holder.clear()
    }
}

fun jsonEscape(value: String): String {
    for (ch in value) {
        when (ch.code) {
            0 -> return execEscape(value)
            1 -> return execEscape(value)
            2 -> return execEscape(value)
            3 -> return execEscape(value)
            4 -> return execEscape(value)
            5 -> return execEscape(value)
            6 -> return execEscape(value)
            7 -> return execEscape(value)
            8 -> return execEscape(value)
            9 -> return execEscape(value)
            10 -> return execEscape(value)
            11 -> return execEscape(value)
            12 -> return execEscape(value)
            13 -> return execEscape(value)
            14 -> return execEscape(value)
            15 -> return execEscape(value)
            16 -> return execEscape(value)
            17 -> return execEscape(value)
            18 -> return execEscape(value)
            19 -> return execEscape(value)
            20 -> return execEscape(value)
            21 -> return execEscape(value)
            22 -> return execEscape(value)
            23 -> return execEscape(value)
            24 -> return execEscape(value)
            25 -> return execEscape(value)
            26 -> return execEscape(value)
            27 -> return execEscape(value)
            28 -> return execEscape(value)
            29 -> return execEscape(value)
            30 -> return execEscape(value)
            31 -> return execEscape(value)
            '\"'.code -> return execEscape(value)
            '\\'.code -> return execEscape(value)
            '\u007f'.code -> return execEscape(value)
        }
    }
    return value
}

private fun execEscape(value: String): String {
    return buildString(value.length) {
        for (ch in value) {
            when (ch) {
                '\"' -> append("\\\"")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\\' -> append("\\\\")
                '\t' -> append("\\t")
                '\b' -> append("\\b")
                '\u0000' -> append("\\u0000")
                '\u0001' -> append("\\u0001")
                '\u0002' -> append("\\u0002")
                '\u0003' -> append("\\u0003")
                '\u0004' -> append("\\u0004")
                '\u0005' -> append("\\u0005")
                '\u0006' -> append("\\u0006")
                '\u0007' -> append("\\u0007")
                '\u000B' -> append("\\u000b")
                '\u000C' -> append("\\u000c")
                '\u000E' -> append("\\u000e")
                '\u000F' -> append("\\u000f")
                '\u0010' -> append("\\u0010")
                '\u0011' -> append("\\u0011")
                '\u0012' -> append("\\u0012")
                '\u0013' -> append("\\u0013")
                '\u0014' -> append("\\u0014")
                '\u0015' -> append("\\u0015")
                '\u0016' -> append("\\u0016")
                '\u0017' -> append("\\u0017")
                '\u0018' -> append("\\u0018")
                '\u0019' -> append("\\u0019")
                '\u001A' -> append("\\u001a")
                '\u001B' -> append("\\u001b")
                '\u001C' -> append("\\u001c")
                '\u001D' -> append("\\u001d")
                '\u001E' -> append("\\u001e")
                '\u001F' -> append("\\u001f")
                '\u007F' -> append("\\u007f")
                else -> append(ch)
            }
        }
    }
}
