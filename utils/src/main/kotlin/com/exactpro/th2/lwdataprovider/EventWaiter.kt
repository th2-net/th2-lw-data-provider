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

import com.exactpro.th2.common.event.EventUtils.toEventID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.utils.event.logId
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.EventResponse
import com.google.protobuf.Timestamp
import io.github.oshai.kotlinlogging.KotlinLogging
import java.lang.System.nanoTime
import java.lang.Thread.sleep
import java.time.Duration
import java.time.Instant
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

fun main() {
    /*
    Exception in thread "main" java.lang.RuntimeException: Can not execute GRPC blocking request
	at com.exactpro.th2.service.AbstractGrpcService.executeWithRetrySync(AbstractGrpcService.java:107)
	at com.exactpro.th2.service.AbstractGrpcService.executeWithRetrySync(AbstractGrpcService.java:136)
	at com.exactpro.th2.service.AbstractGrpcService.createBlockingRequest(AbstractGrpcService.java:69)
	at com.exactpro.th2.dataprovider.lw.grpc.DataProviderDefaultBlockingImpl.getEvent(DataProviderDefaultBlockingImpl.java:27)
	at com.exactpro.th2.dataprovider.lw.grpc.DataProviderDefaultBlockingImpl.getEvent(DataProviderDefaultBlockingImpl.java:31)
	at com.exactpro.th2.lwdataprovider.EventWaiterKt.main(EventWaiter.kt:37)
	at com.exactpro.th2.lwdataprovider.EventWaiterKt.main(EventWaiter.kt)
	Suppressed: io.grpc.StatusRuntimeException: UNKNOWN
		at io.grpc.stub.ClientCalls.toStatusRuntimeException(ClientCalls.java:351)
		at io.grpc.stub.ClientCalls.getUnchecked(ClientCalls.java:332)
		at io.grpc.stub.ClientCalls.blockingUnaryCall(ClientCalls.java:174)
		at com.exactpro.th2.dataprovider.lw.grpc.DataProviderGrpc$DataProviderBlockingStub.getEvent(DataProviderGrpc.java:644)
		at com.exactpro.th2.dataprovider.lw.grpc.DataProviderDefaultBlockingImpl.lambda$getEvent$0(DataProviderDefaultBlockingImpl.java:27)
		at com.exactpro.th2.service.AbstractGrpcService.executeWithRetrySync(AbstractGrpcService.java:99)
		... 6 more
test_book:script:20240927071644715909400:714feeaf-7ca0-11ef-aebf-355a233bacbb5

     */
    CommonFactory.createFromArguments("-c", "cfg").use { factory ->
        println(Instant.now())
        factory.grpcRouter.getService(DataProviderService::class.java).getEvent(toEventID(
            Instant.parse("2024-09-27T07:16:44Z").plus(715_909_400, ChronoUnit.NANOS),
            "test_book",
            "script",
            "714feeaf-7ca0-11ef-aebf-355a233bacbb5"
        ))
    }
}