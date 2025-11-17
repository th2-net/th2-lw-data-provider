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