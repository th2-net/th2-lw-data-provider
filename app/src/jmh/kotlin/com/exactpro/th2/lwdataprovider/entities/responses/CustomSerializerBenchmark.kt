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
import com.exactpro.cradle.Direction
import com.exactpro.cradle.PageId
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.BatchedStoredTestEvent
import com.exactpro.cradle.testevents.BatchedStoredTestEventBuilder
import com.exactpro.cradle.testevents.StoredTestEventBatch
import com.exactpro.th2.lwdataprovider.MapEscaper
import com.exactpro.th2.lwdataprovider.entities.responses.ser.HeapBufferPool
import com.exactpro.th2.lwdataprovider.entities.responses.ser.UnpooledBufPool
import com.exactpro.th2.lwdataprovider.entities.responses.ser.calculateSize
import com.exactpro.th2.lwdataprovider.entities.responses.ser.serialize
import com.exactpro.th2.lwdataprovider.entities.responses.ser.serializeJsonData
import org.apache.commons.lang3.RandomStringUtils
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Scope.Thread
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import java.nio.ByteBuffer
import java.time.Instant

@Suppress("unused")
@State(Scope.Benchmark)
open class CustomSerializerBenchmark {
    @State(Thread)
    open class Simple {
        val bufPool = UnpooledBufPool()
        val bufferPool = HeapBufferPool()
        val escaper = MapEscaper()

        val timestamp: Instant = Instant.now()
        val bookId = BookId("benchmark-batch-id")
        val msgId = StoredMessageId(
            bookId, "benchmark-session-alias", Direction.SECOND,
            timestamp, 0L
        )
        lateinit var largeEvent: Event

        @Setup
        open fun init() {
            val pageId = PageId(bookId, timestamp, "")
            val scope = "benchmark-scope"
            val eventId = StoredTestEventId(bookId, scope, timestamp, "benchmark-event-id")
            val batchId = StoredTestEventId(bookId, scope, timestamp, "benchmark-batch-event-id")
            largeEvent = Event(
                event = BatchedStoredTestEventBuilder()
                    .setId(eventId)
                    .setName("benchmark-name")
                    .setType("benchmark-type")
                    .setParentId(StoredTestEventId(bookId, scope, timestamp, "benchmark-parent-event-id"))
                    .setEndTimestamp(timestamp)
                    .setSuccess(true)
                    .setContent(
                        ByteBuffer.wrap(
                            """["body":"{${
                                RandomStringUtils.insecure().nextAlphabetic(600_000)
                            }"}]""".toByteArray(Charsets.UTF_8)
                        )
                    )
                    .setBatch(
                        StoredTestEventBatch(
                            batchId,
                            "benchmark-batch-name",
                            "benchmark-batch-type",
                            StoredTestEventId(bookId, scope, timestamp, "benchmark-batch-parent-event-id"),
                            emptyList<BatchedStoredTestEvent>(),
                            mapOf(
                                eventId to setOf(
                                    msgId
                                )
                            ),
                            pageId,
                            "",
                            timestamp
                        )
                    )
                    .setPageId(pageId)
                    .build(),
                batchId = batchId,
                parentBatchId = batchId,
            )
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    fun benchmarkSerializeEventUsingByteBuffer(
        state: Simple,
    ) {
        val buffer =
            serialize(state.bufferPool.acquire(1_024 * 1_024), state.escaper, state.largeEvent::serializeJsonData)
        state.bufferPool.release(buffer)
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    fun benchmarkSerializeEventUsingByteBuf(
        state: Simple,
    ) {
        val buf = serialize(state.bufPool.acquire(1_024 * 2), state.escaper, state.largeEvent::serializeJsonData)
        state.bufPool.release(buf)
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    fun benchmarkCalculateSizeOfSerializeEvent(
        state: Simple,
    ) {
        calculateSize(state.largeEvent::serializeJsonData)
    }
}
