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

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.utils.event.logId
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.EventResponse
import com.google.protobuf.Timestamp
import io.github.oshai.kotlinlogging.KotlinLogging
import java.lang.System.nanoTime
import java.lang.Thread.sleep
import java.time.Duration
import java.time.temporal.ChronoUnit
import kotlin.math.min

class EventWaiter(
    private val service: DataProviderService
) {
    @JvmOverloads
    @Throws(InterruptedException::class)
    fun waitEventResponseOrNull(id: EventID, duration: Duration, pullingInterval: Duration = Duration.of(100, ChronoUnit.MILLIS)): EventResponse? {
        id.verify()
        require(!duration.isNegative) { "'duration' shouldn't be negative" }
        require(!pullingInterval.isNegative) { "'pulling interval' shouldn't be negative" }

        val startTime = nanoTime()

        var response: EventResponse? = getEventResponseInternal(id)

        if (response != null) return response
        if (duration == Duration.ZERO) return null

        val timeout: Long = duration.toNanos()
        val endTime: Long = startTime + timeout
        val interval: Long = min(pullingInterval.toNanos(), timeout)

        while (nanoTime() < endTime) {
            response = getEventResponseInternal(id)
            if (response != null) return response
            val sleepTime = min(endTime - nanoTime(), interval)
            if (sleepTime < 0) return null
            sleep(sleepTime / 1_000_000, (sleepTime % 1_000_000).toInt())
        }
        return null
    }

    fun getEventResponseOrNull(id: EventID): EventResponse? {
        id.verify()
        return getEventResponseInternal(id)
    }

    private fun getEventResponseInternal(id: EventID): EventResponse? {
        return try {
            service.getEvent(id)
        } catch (e: RuntimeException) {
            LOGGER.warn(e) { "Event id '${id.logId}'" }
            null
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private fun EventID.verify() {
            require(bookName.isNotBlank()) { "'bookName' shouldn't be blank" }
            require(scope.isNotBlank()) { "'scope' shouldn't be blank" }
            require(startTimestamp != Timestamp.getDefaultInstance()) { "'start timestamp' shouldn't be default instance" }
            require(id.isNotBlank()) { "'id' shouldn't be blank" }
        }
    }
}