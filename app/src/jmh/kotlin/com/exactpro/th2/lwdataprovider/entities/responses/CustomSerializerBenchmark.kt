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

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.BookId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import org.apache.commons.lang3.RandomStringUtils
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Scope.Thread
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole
import java.nio.ByteBuffer
import java.time.Instant

@Suppress("unused")
@State(Scope.Benchmark)
open class CustomSerializerBenchmark {
    @State(Thread)
    open class Simple {
        lateinit var largeEvent: Event

        @Setup
        open fun init() {
            val timestamp = Instant.now()
            largeEvent =
                Event(
                    eventId = "eventId",
                    batchId = "batchId",
                    shortEventId = "shortEventId",
                    isBatched = true,
                    eventName = "eventName",
                    eventType = "eventType",
                    endTimestamp = timestamp,
                    startTimestamp = timestamp,
                    parentEventId =
                        ProviderEventId(
                            batchId =
                                StoredTestEventId(
                                    BookId("bookId"),
                                    "scope",
                                    timestamp,
                                    "id",
                                ),
                            eventId =
                                StoredTestEventId(
                                    BookId("bookId"),
                                    "scope",
                                    timestamp,
                                    "id",
                                ),
                        ),
                    successful = true,
                    bookId = "bookId",
                    scope = "scope",
                    attachedMessageIds =
                        setOf(
                            "attachedMessageId",
                        ),
                    body = ByteBuffer.wrap("""["body":"{${RandomStringUtils.insecure().nextAlphabetic(600_000)}"}]""".toByteArray(Charsets.UTF_8)),
                )
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    fun benchmarkIncrementTotalMetricsOldVsSimpleBatch(
        blackhole: Blackhole,
        state: Simple,
    ) {
        blackhole.consume(state.largeEvent.toJSONByteArray())
    }
}
