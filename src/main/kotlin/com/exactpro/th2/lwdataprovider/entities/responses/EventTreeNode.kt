/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.EventMetadata
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.fasterxml.jackson.annotation.JsonIgnore
import mu.KotlinLogging
import java.time.Instant

data class EventTreeNode(
    val type: String = "eventTreeNode",

    val eventName: String,
    val eventType: String,
    val successful: Boolean,
    val startTimestamp: Instant,

    @JsonIgnore
    var parentEventId: ProviderEventId?,

    @JsonIgnore
    val id: ProviderEventId
) {

    val eventId: String
        get() = id.toString()

    val parentId: String?
        get() = parentEventId?.toString()

    companion object {
        private const val error = "field is null in both batched and non-batched event metadata"
        private val logger = KotlinLogging.logger { }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as EventTreeNode

        if (id.eventId != other.id.eventId) return false

        return true
    }

    override fun hashCode(): Int {
        return id.eventId.hashCode()
    }


    fun convertToGrpcEventMetadata(): EventMetadata {
        return EventMetadata.newBuilder()
            .setEventId(EventID.newBuilder().setId(eventId))
            .setEventName(eventName)
            .setEventType(eventType)
            .setStartTimestamp(startTimestamp.toTimestamp())
            .setStatus(if (successful) EventStatus.SUCCESS else EventStatus.FAILED)
            .also { builder ->
                parentEventId?.let { builder.setParentEventId(EventID.newBuilder().setId(parentId)) }
            }.build()
    }

}