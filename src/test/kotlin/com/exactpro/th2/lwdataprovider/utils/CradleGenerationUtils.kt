/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.utils

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.MessageToStore
import com.exactpro.cradle.messages.MessageToStoreBuilder
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.lwdataprovider.grpc.toGrpcDirection
import java.time.Duration
import java.time.Instant

fun createStoredMessages(
    alias: String,
    startSequence: Long,
    startTimestamp: Instant,
    count: Long,
    direction: Direction = Direction.FIRST,
    incSeconds: Long = 10L,
    maxTimestamp: Instant,
    bookId: BookId = BookId("test-book"),
): List<StoredMessage> {
    return createMessageToStore(alias, startSequence, startTimestamp, count, direction, Duration.ofSeconds(incSeconds), maxTimestamp, bookId)
        .map { msg ->
        StoredMessage(msg, msg.id, null)
    }.toList()
}

fun createMessageToStore(
    alias: String,
    startSequence: Long,
    startTimestamp: Instant,
    count: Long,
    direction: Direction = Direction.FIRST,
    increment: Duration = Duration.ofSeconds(10L),
    maxTimestamp: Instant,
    bookId: BookId = BookId("test-book"),
): Sequence<MessageToStore> {
    return (0 until count).asSequence().map {
        val index = startSequence + it
        val instant = startTimestamp.plus(increment * it).coerceAtMost(maxTimestamp)
        MessageToStoreBuilder()
            .bookId(bookId)
            .direction(direction)
            .sessionAlias(alias)
            .sequence(index)
            .timestamp(instant)
            .content(
                RawMessage.newBuilder().apply {
                    metadataBuilder.apply {
                        id = MessageID.newBuilder()
                            .setBookName(bookId.name)
                            .setDirection(direction.toGrpcDirection())
                            .setSequence(index)
                            .setConnectionId(ConnectionID.newBuilder().setSessionAlias(alias))
                            .setTimestamp(instant.toTimestamp())
                            .build()
                    }
                }.build().toByteArray()
            )
            .build()
    }
}

private operator fun Duration.times(multiplier: Long): Duration {
    return Duration.ofNanos(toNanos() * multiplier)
}

fun generateMessageSequence(): Long = Instant.now().run { epochSecond * 1_000_000_000 + nano }