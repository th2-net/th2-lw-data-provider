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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.event.EventUtils.generateUUID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.EventResponse
import com.google.protobuf.util.Timestamps
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertNull

class EventWaiterTest {
    private val service: DataProviderService = mock { }
    private val waiter = EventWaiter(service)

    @Test
    fun `wait event not null result`() {
        whenever(service.getEvent(EVENT_ID)).thenReturn(EVENT_RESPONSE)
        assertEquals(EVENT_RESPONSE, waiter.waitEventResponseOrNull(EVENT_ID, Duration.ofSeconds(1)))
    }

    @Test
    fun `wait event not null result with zero duration`() {
        whenever(service.getEvent(EVENT_ID)).thenReturn(EVENT_RESPONSE)
        assertEquals(EVENT_RESPONSE, waiter.waitEventResponseOrNull(EVENT_ID, Duration.ZERO))
    }

    @Test
    fun `wait event not null result on second time`() {
        val firstInvoke = AtomicBoolean(true)
        whenever(service.getEvent(EVENT_ID)).thenAnswer {
            if (firstInvoke.compareAndSet(true, false)) {
                throw EXCEPTION
            }
            return@thenAnswer EVENT_RESPONSE
        }
        assertEquals(EVENT_RESPONSE, waiter.waitEventResponseOrNull(EVENT_ID, Duration.ofSeconds(1)))
        verify(service, times(2)).getEvent(EVENT_ID)
    }

    @Test
    fun `wait event null result when gRPC throws exception`() {
        whenever(service.getEvent(EVENT_ID)).doThrow(EXCEPTION)
        assertNull(waiter.waitEventResponseOrNull(EVENT_ID, Duration.ofMillis(150), Duration.ofMillis(10)))
        verify(service, atLeast(5)).getEvent(EVENT_ID)
    }

    @Test
    fun `wait event null result when gRPC returns null`() {
        assertNull(waiter.waitEventResponseOrNull(EVENT_ID, Duration.ofMillis(150), Duration.ofMillis(10)))
        verify(service, atLeast(5)).getEvent(EVENT_ID)
    }



    @Test
    fun `get event not null result`() {
        whenever(service.getEvent(EVENT_ID)).thenReturn(EVENT_RESPONSE)
        assertEquals(EVENT_RESPONSE, waiter.getEventResponseOrNull(EVENT_ID))
        verify(service).getEvent(EVENT_ID)
    }

    @Test
    fun `get event null result when gRPC throws exception`() {
        whenever(service.getEvent(EVENT_ID)).doThrow(EXCEPTION)
        assertNull(waiter.getEventResponseOrNull(EVENT_ID))
        verify(service).getEvent(EVENT_ID)
    }

    @Test
    fun `get event null result when gRPC returns null`() {
        assertNull(waiter.getEventResponseOrNull(EVENT_ID))
        verify(service).getEvent(EVENT_ID)
    }

    companion object {
        private val EXCEPTION = RuntimeException("test-error")
        private val EVENT_ID = EventID.newBuilder()
            .setBookName("test-book")
            .setScope("test-scope")
            .setStartTimestamp(Timestamps.now())
            .setId(generateUUID())
            .build()
        private val EVENT_RESPONSE = EventResponse.newBuilder()
            .setEventId(EVENT_ID)
            .build()
    }
}