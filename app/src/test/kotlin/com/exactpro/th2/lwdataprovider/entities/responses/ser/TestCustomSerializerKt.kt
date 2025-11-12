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

package com.exactpro.th2.lwdataprovider.entities.responses.ser

import com.exactpro.cradle.BookId
import com.exactpro.cradle.PageId
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.BatchedStoredTestEvent
import com.exactpro.cradle.testevents.BatchedStoredTestEventBuilder
import com.exactpro.cradle.testevents.StoredTestEventBatch
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.lwdataprovider.DummyEscaper
import com.exactpro.th2.lwdataprovider.MapEscaper
import com.exactpro.th2.lwdataprovider.entities.internal.Direction
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53Transport
import com.exactpro.th2.lwdataprovider.entities.responses.TransportMessageContainer
import com.fasterxml.jackson.databind.json.JsonMapper
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.nio.ByteBuffer
import java.time.Instant
import kotlin.text.Charsets.UTF_8

internal class TestCustomSerializerKt {
    private val mapper = JsonMapper()

    @ParameterizedTest(name = "char `{0}` does not cause problems")
    @ValueSource(chars = ['\"', '\\', ':'])
    @MethodSource("controlChars")
    @MethodSource("unicodeChars")
    fun `writes ProviderMessage53Transport as valid json (ByteBuf)`(escapeCharacter: Char) {
        val message = createMessage(escapeCharacter)
        val buf: ByteBuf = serialize(Unpooled.buffer(), DummyEscaper, message::serializeJsonData)
        assertDoesNotThrow { mapper.readTree(buf.toByteArray()) }
    }

    @ParameterizedTest(name = "char `{0}` does not cause problems")
    @ValueSource(chars = ['\"', '\\', ':'])
    @MethodSource("controlChars")
    @MethodSource("unicodeChars")
    fun `writes ProviderMessage53Transport as valid json (ByteBuffer)`(escapeCharacter: Char) {
        val message = createMessage(escapeCharacter)
        val buffer: ByteBuffer = serialize(ByteBuffer.allocate(calculateSize(message::serializeJsonData)), DummyEscaper, message::serializeJsonData)
        assertDoesNotThrow { mapper.readTree(buffer.array()) }
    }

    @ParameterizedTest(name = "char `{0}` does not cause problems")
    @ValueSource(chars = ['\"', '\\', ':'])
    @MethodSource("controlChars")
    @MethodSource("unicodeChars")
    fun `writes Event as valid json (ByteBuf)`(escapeCharacter: Char) {
        val event = createEvent(escapeCharacter)
        val buf: ByteBuf = serialize(Unpooled.buffer(), DummyEscaper, event::serializeJsonData)
        assertDoesNotThrow { mapper.readTree(buf.toByteArray()) }
    }

    @ParameterizedTest(name = "char `{0}` does not cause problems")
    @ValueSource(chars = ['\"', '\\', ':'])
    @MethodSource("controlChars")
    @MethodSource("unicodeChars")
    fun `writes Event as valid json (ByteBuffer)`(escapeCharacter: Char) {
        val event = createEvent(escapeCharacter)
        val buffer: ByteBuffer = serialize(ByteBuffer.allocate(calculateSize(event::serializeJsonData)), DummyEscaper, event::serializeJsonData)
        assertDoesNotThrow { mapper.readTree(buffer.array()) }
    }

    @ParameterizedTest(name = "char `{0}` escaped as `{1}`")
    @MethodSource("escapedResults")
    fun `test json escape result`(char: Char, escaped: String) {
        expectThat(MapEscaper().escape("$char", false)).isEqualTo(escaped.toByteArray(UTF_8))
    }

    private fun createMessage(
        escapeCharacter: Char
    ): ProviderMessage53Transport {
        val timestamp = Instant.now()
        return ProviderMessage53Transport(
            timestamp = timestamp,
            direction = Direction.OUT,
            sessionId = "ses${escapeCharacter}sion",
            attachedEventIds = setOf(
                "eve${escapeCharacter}nt",
            ),
            bodyBytes = byteArrayOf(42, 43),
            messageId = StoredMessageId(
                BookId("bo${escapeCharacter}ok"),
                "session${escapeCharacter}Alias",
                com.exactpro.cradle.Direction.SECOND,
                timestamp,
                42L,
            ),
            body = listOf(
                TransportMessageContainer(
                    sessionGroup = "session${escapeCharacter}Group",
                    parsedMessage = ParsedMessage(
                        id = MessageId(
                            sessionAlias = "session${escapeCharacter}Alias",
                            direction = com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.OUTGOING,
                            sequence = 42L,
                            timestamp = timestamp,
                        ),
                        eventId = EventId(
                            id = "eve${escapeCharacter}nt",
                            scope = "scop${escapeCharacter}e",
                            timestamp = timestamp,
                            book = "bo${escapeCharacter}ok",
                        ),
                        type = "Message${escapeCharacter}Type",
                        metadata = mapOf(
                            "ke${escapeCharacter}y" to "val${escapeCharacter}ue",
                        ),
                        protocol = "proto${escapeCharacter}col",
                        rawBody = Unpooled.wrappedBuffer(
                            """{"test":42}""".toByteArray(UTF_8)
                        ),
                    )
                ),
            ),
        )
    }

    private fun createEvent(
        escapeCharacter: Char,
    ): Event {
        val timestamp = Instant.now()
        val bookId = BookId("book${escapeCharacter}Id")
        val scope = "scope${escapeCharacter}"
        val eventId = StoredTestEventId(bookId, scope, timestamp, "event${escapeCharacter}Id")
        val batchId = StoredTestEventId(bookId, scope, timestamp, "event${escapeCharacter}Id")
        val pageId = PageId(bookId, timestamp, "")
        return Event(
            event = BatchedStoredTestEventBuilder()
                .setId(eventId)
                .setName("event${escapeCharacter}Name")
                .setType("event${escapeCharacter}Type")
                .setParentId(StoredTestEventId(bookId, scope, timestamp, "event${escapeCharacter}Id"))
                .setEndTimestamp(timestamp)
                .setSuccess(true)
                .setContent(ByteBuffer.wrap("""[{"body":"test-body"}]""".toByteArray(UTF_8)))
                .setBatch(
                StoredTestEventBatch(
                    batchId,
                    "test-batch-name",
                    "test-batch-type",
                    StoredTestEventId(bookId, scope, timestamp, "event${escapeCharacter}Id"),
                    emptyList<BatchedStoredTestEvent>(),
                    mapOf(
                        eventId to setOf(
                            StoredMessageId(
                                bookId, "attachedMessage${escapeCharacter}Id", com.exactpro.cradle.Direction.SECOND,
                                timestamp, 0L
                            )
                        )
                    ),
                    pageId,
                    "",
                    timestamp
                ))
                .setPageId(pageId)
                .build(),
            batchId = batchId,
            parentBatchId = batchId,
        )
    }

    companion object {
        @JvmStatic
        fun controlChars(): List<Arguments> =
            // 0x00 (NUL)..0x1F (US) + 0x7F (DEL)
            ((0..' '.code - 1) + 0x7f).map { arguments(it.toChar()) }

        @JvmStatic
        fun unicodeChars(): List<Arguments> =
            listOf(
                arguments('a'), // 1 byte
                arguments('¡'), // 2 bytes
                arguments('Ⴔ'), // 3 bytes
                arguments("🦛"[0]), // half of 4 bytes
            )

        @JvmStatic
        fun escapedResults(): List<Arguments> =
            listOf(
                arguments('\u0000', "\\u0000"),
                arguments('\u000F', "\\u000f"),
                arguments('\u0010', "\\u0010"),
                arguments('\u001F', "\\u001f"),
                arguments('\b', "\\b"),
                arguments('\n', "\\n"),
                arguments('\r', "\\r"),
                arguments('\t', "\\t"),
                arguments('\"', "\\\""),
                arguments('\\', "\\\\"),
                arguments(':', ":"),
                arguments(' ', " "),
                arguments('~', "~"),
                arguments('\u007F', "\\u007f"),
            )
    }
}