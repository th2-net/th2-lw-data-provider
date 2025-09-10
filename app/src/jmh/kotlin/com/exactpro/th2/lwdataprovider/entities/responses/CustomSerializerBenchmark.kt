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
import com.exactpro.cradle.PageId
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.lw.LwBatchedStoredTestEvent
import com.exactpro.cradle.testevents.lw.LwStoredTestEventBatch
import com.exactpro.th2.lwdataprovider.DummyEscaper
import com.exactpro.th2.lwdataprovider.MapEscaper
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
import java.io.OutputStream
import java.nio.ByteBuffer
import java.time.Instant

@Suppress("unused")
@State(Scope.Benchmark)
open class CustomSerializerBenchmark {
    @State(Thread)
    open class Simple {
        val bufPool = UnpooledBufPool()
        val buf = ByteBuffer.allocate(1_024 * 1_024)
        val escaper = MapEscaper()

        val timestamp = Instant.now()
        val bookId = BookId("benchmark-batch-id")
        val msgId = StoredMessageId(bookId, "benchmark-session-alias", com.exactpro.cradle.Direction.SECOND,
            timestamp, 0L)
        lateinit var largeEvent: LwEvent

        @Setup
        open fun init() {
            val pageId = PageId(bookId, timestamp, "")
            val scope = "benchmark-scope"
            val eventId = StoredTestEventId(bookId, scope, timestamp, "benchmark-event-id")
            val batchId = StoredTestEventId(bookId, scope, timestamp, "benchmark-batch-event-id")
            largeEvent = LwEvent(
                event = LwBatchedStoredTestEvent(
                    eventId,
                    "benchmark-name",
                    "benchmark-type",
                    StoredTestEventId(bookId, scope, timestamp, "benchmark-parent-event-id"),
                    timestamp,
                    true,
                    ByteBuffer.wrap("""["body":"{${RandomStringUtils.insecure().nextAlphabetic(600_000)}"}]""".toByteArray(Charsets.UTF_8)),
                    LwStoredTestEventBatch(
                        batchId,
                        "benchmark-batch-name",
                        "benchmark-batch-type",
                        StoredTestEventId(bookId, scope, timestamp, "benchmark-batch-parent-event-id"),
                        emptyList<LwBatchedStoredTestEvent>(),
                        mapOf(
                            eventId to setOf(
                                msgId
                            )
                        ),
                        pageId,
                        "",
                        timestamp
                    ),
                    pageId,
                ),
                batchId = batchId,
                parentBatchId = batchId,
            )
        }
    }

//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    fun benchmarkIncrementTotalMetricsOldVsSimpleBatch(
//        state: Simple,
//    ) {
//        state.bufPool.release(state.largeEvent.writeJsonData(state.bufPool.acquire(), state.escaper))
//    }

//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    fun benchmarkIncrementTotalMetricsOldVsSimpleBatch(
//        state: Simple,
//    ) {
//        state.largeEvent.putJsonData(state.buf, state.escaper)
//        state.buf.clear()
//    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    fun benchmarkIncrementTotalMetricsOldVsSimpleBatch(
        state: Simple,
    ) {
        state.buf.putMessageId(state.msgId, state.escaper)
        state.buf.clear()
    }

    companion object {
        object BlackholeOutputStream : OutputStream() {
            override fun write(b: Int) = Unit
            override fun write(b: ByteArray?, off: Int, len: Int) = Unit
        }
    }
}
