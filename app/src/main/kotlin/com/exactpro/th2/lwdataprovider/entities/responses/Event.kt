/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.TestEventSingle
import com.exactpro.th2.common.event.EventUtils.toEventID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus.FAILED
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.EventResponse
import com.exactpro.th2.lwdataprovider.grpc.toGrpcMessageId
import com.google.protobuf.UnsafeByteOperations
import com.exactpro.th2.common.grpc.Event as CommonGrpcEvent

data class Event(
    val event: TestEventSingle,
    val batchId: StoredTestEventId? = null,
    val parentBatchId: StoredTestEventId? = null,
) {
    val eventId: StoredTestEventId
        get() = event.id

    val bookId: String
        get() = event.id.bookId.name

    val scope: String
        get() = event.id.scope

    val attachedMessageIds: Set<StoredMessageId>
        get() = event.messages ?: emptySet()

    val isBatched: Boolean
        get() = batchId != null

    fun toGrpcEventResponse(): EventResponse = with(event) {
        return EventResponse.newBuilder()
            .setEventId(id.toEventIdProto())
            .setIsBatched(isBatched)
            .setEventName(name)
            .setStartTimestamp(startTimestamp.toTimestamp())
            .setStatus(if (isSuccess) SUCCESS else FAILED)
            .also { builder ->
                batchId?.let { builder.batchId = it.toEventIdProto() }
                parentId?.let { builder.parentEventId = it.toEventIdProto() }
                type?.let { builder.eventType = it }
                endTimestamp?.let { builder.endTimestamp = it.toTimestamp() }
                attachedMessageIds.forEach {
                    builder.addAttachedMessageId(it.toGrpcMessageId())
                }
                contentBuffer?.let { builder.body = UnsafeByteOperations.unsafeWrap(it) }
            }.build()
    }

    fun toGrpcEvent(): CommonGrpcEvent = with(event) {
        return CommonGrpcEvent.newBuilder()
            .setId(id.toEventIdProto())
            .setName(name)
            .setType(type)
            .setStatus(if (isSuccess) SUCCESS else FAILED)
            .also { builder ->
                parentId?.also { builder.parentId = it.toEventIdProto() }
                type?.let { builder.type = it }
                endTimestamp?.also { builder.endTimestamp = it.toTimestamp() }
                attachedMessageIds.forEach {
                    builder.addAttachedMessageIds(it.toGrpcMessageId())
                }
                contentBuffer?.let { builder.body = UnsafeByteOperations.unsafeWrap(it) }
            }
            .build()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is com.exactpro.th2.lwdataprovider.entities.responses.Event) return false

        if (batchId != other.batchId) return false
        if (event != other.event) return false

        return true
    }

    override fun hashCode(): Int {
        var result = batchId?.hashCode() ?: 0
        result = 31 * result + event.hashCode()
        return result
    }

    companion object {
        fun StoredTestEventId.toEventIdProto(): EventID = toEventID(startTimestamp, bookId.name, scope, id)
    }
}
