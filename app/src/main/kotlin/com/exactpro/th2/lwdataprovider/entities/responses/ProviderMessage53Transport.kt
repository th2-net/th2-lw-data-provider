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

import com.exactpro.cradle.Direction.FIRST
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.entities.internal.Direction
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.time.Instant
import java.util.*

@Deprecated("same format as rpt-data-provider5.3")
@Serializable
data class ProviderMessage53Transport(
    @Serializable(with = InstantSerializer::class) val timestamp: Instant,
    val direction: Direction?,
    val sessionId: String,
//    val messageType: String?,

    val attachedEventIds: Set<String>,


    val body: List<TransportMessageContainer>?,

    @SerialName("bodyBase64")
    @Serializable(with = ByteArrayAsBase64Serializer::class)
    val bodyBytes: ByteArray? = null,

    @Serializable(with = StoredMessageIdSerializer::class)
    val messageId: StoredMessageId,
) : ResponseMessage {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ProviderMessage53Transport) return false

        if (timestamp != other.timestamp) return false
        if (direction != other.direction) return false
        if (sessionId != other.sessionId) return false
        if (attachedEventIds != other.attachedEventIds) return false
        if (body != other.body) return false
        if (!bodyBytes.contentEquals(other.bodyBytes)) return false
        if (messageId != other.messageId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = timestamp.hashCode()
        result = 31 * result + (direction?.hashCode() ?: 0)
        result = 31 * result + sessionId.hashCode()
        result = 31 * result + attachedEventIds.hashCode()
        result = 31 * result + (body?.hashCode() ?: 0)
        result = 31 * result + (bodyBytes?.contentHashCode() ?: 0)
        result = 31 * result + messageId.hashCode()
        return result
    }

    companion object {
        fun create(
            rawStoredMessage: StoredMessage,
            sessionGroup: String,
            body: List<ParsedMessage>?,
            bodyBytes: ByteArray?,
            events: Set<String> = Collections.emptySet()
        ): ProviderMessage53Transport {
            return ProviderMessage53Transport(
                timestamp = rawStoredMessage.timestamp ?: Instant.ofEpochMilli(0),
                direction = Direction.fromStored(rawStoredMessage.direction ?: FIRST),
                sessionId = rawStoredMessage.sessionAlias ?: "",
//        messageType = body?.type,
                attachedEventIds = events,
                body = body?.map { TransportMessageContainer(sessionGroup, it) },
                bodyBytes = bodyBytes,
                messageId = rawStoredMessage.id
            )
        }
    }
}
