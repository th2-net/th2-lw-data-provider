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

package com.exactpro.th2.lwdataprovider.http.util

import com.exactpro.th2.lwdataprovider.EventType
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.handlers.AbstractCancelableHandler
import com.exactpro.th2.lwdataprovider.http.listener.ProgressListener
import com.exactpro.th2.lwdataprovider.metrics.HttpWriteMetrics
import com.exactpro.th2.lwdataprovider.metrics.Metric
import com.exactpro.th2.lwdataprovider.metrics.ResponseQueue
import io.github.oshai.kotlinlogging.KLogger
import io.javalin.http.Context
import io.javalin.http.Header
import io.javalin.http.HttpStatus
import io.prometheus.client.SimpleTimer
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Supplier

const val JSON_STREAM_CONTENT_TYPE = "application/stream+json"

fun writeJsonStream(
    ctx: Context,
    queue: ArrayBlockingQueue<Supplier<SseEvent>>,
    handler: AbstractCancelableHandler,
    metric: Metric,
    logger: KLogger,
    progressListener: ProgressListener,
    bufferSize: Int
) {
    progressListener.onStart()

    val matchedPath = ctx.matchedPath()
    val queueSizeMetric = ResponseQueue.queueSizeMetric(matchedPath)
    var dataSent = 0

    var writeHeader = true
    var status: HttpStatus = HttpStatus.OK

    val output = ctx.res().apply {
        logger.info { "before ${this.bufferSize}" }
        if (bufferSize > 0) {
            this.bufferSize = bufferSize
        }
        logger.info { "after ${this.bufferSize}" }
    }.outputStream

    try {
        val processSseEventMetric = metric.child("process_sse_event")
        val writeSseEventMeasurement = metric.child("write_sse_event")
        do {
            val startNanos = System.nanoTime()
            try {
                val nextEvent = queue.take()
                queueSizeMetric.set(queue.size.toDouble())
                val sseEvent = nextEvent.get()
                if (writeHeader && sseEvent is SseEvent.ErrorData.SimpleError) {
                    // something happened during request
                    status = HttpStatus.INTERNAL_SERVER_ERROR
                }
                if (writeHeader) {
                    ctx.status(status)
                        .contentType(JSON_STREAM_CONTENT_TYPE)
                        .header(Header.TRANSFER_ENCODING, "chunked")
                    writeHeader = false
                }
                if (sseEvent is SseEvent.ErrorData) {
                    progressListener.onError(sseEvent)
                }
                if (sseEvent.event == EventType.KEEP_ALIVE) {
                    output.flush()
                } else if (sseEvent.event == EventType.CLOSE) {
                    logger.info { "Received close event" }
                    return
                } else {
                    logger.debug {
                        "Write event to output: " // FIXME: log data
                    }
                    val startWrite = System.nanoTime()
                    try {
                        sseEvent.writeData(output)
                        output.write('\n'.code)
                        dataSent++
                    } finally {
                        writeSseEventMeasurement.observe(SimpleTimer.elapsedSecondsFromNanos(startWrite, System.nanoTime()))
                    }
                }
                if (queue.isEmpty() && !handler.isAlive) {
                    logger.info { "Request canceled" }
                    return
                }
            } finally {
                processSseEventMetric.observe(SimpleTimer.elapsedSecondsFromNanos(startNanos, System.nanoTime()))
            }
        } while (true)
    } catch (ex: Exception) {
        logger.error(ex) { "cannot process next event" }
        progressListener.onError(ex)
        handler.cancel()
        queue.clear()
    } finally {
        if (handler.isAlive) {
            progressListener.onCompleted()
        } else {
            progressListener.onCanceled()
        }
        HttpWriteMetrics.messageSent(matchedPath, dataSent)
        try {
            output.flush()
        } catch (e: Exception) {
            logger.error(e) { "cannot flush the remaining data when processing is finished" }
        }
    }
}