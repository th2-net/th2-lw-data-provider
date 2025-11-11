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
import com.exactpro.th2.lwdataprovider.db.ChildDataMeasurement
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.handlers.AbstractCancelableHandler
import com.exactpro.th2.lwdataprovider.http.listener.DEFAULT_PROCESS_LISTENER
import com.exactpro.th2.lwdataprovider.http.listener.ProgressListener
import com.exactpro.th2.lwdataprovider.metrics.HttpWriteMetrics
import com.exactpro.th2.lwdataprovider.metrics.ResponseQueue
import io.github.oshai.kotlinlogging.KLogger
import io.javalin.http.Context
import io.javalin.http.Header
import io.javalin.http.HttpStatus
import java.io.OutputStream
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Supplier

const val JSON_STREAM_CONTENT_TYPE = "application/stream+json"

fun writeJsonStream(
    ctx: Context,
    queue: ArrayBlockingQueue<Supplier<SseEvent>>,
    handler: AbstractCancelableHandler,
    dataMeasurement: DataMeasurement,
    logger: KLogger,
    progressListener: ProgressListener = DEFAULT_PROCESS_LISTENER,
    bufferSize: Int = DEFAULT_BUFFER_SIZE
) {
    progressListener.onStart()

    val matchedPath = ctx.matchedPath()
    var dataSent = 0

    var writeHeader = true
    val status: HttpStatus = HttpStatus.OK

    val output = ctx.res().outputStream.let {
        if (bufferSize > 0) {
            it.buffered(bufferSize)
        } else {
            it
        }
    }
    try {
        val awaitConvertToJsonMeasurement = dataMeasurement.child("await_convert_to_json")
        val awaitNextMeasurement = dataMeasurement.child("await_next_sse_event")
        val processSseEventMeasurement = dataMeasurement.child("process_sse_event")
        val writeSseEventMeasurement = dataMeasurement.child("write_sse_event")
        do {
            val pseMeasurement = processSseEventMeasurement.start()
            try {
                val nextEvent = awaitNextEvent(awaitNextMeasurement, queue)
                updateQueueMetric(matchedPath, queue)
                val sseEvent = awaitConver(awaitConvertToJsonMeasurement, nextEvent)
                writeHeader = writeHeader(writeHeader, sseEvent, status, ctx)
                processErrorData(sseEvent, progressListener)
                if (sseEvent.event == EventType.KEEP_ALIVE) {
                    flush(output)
                } else if (sseEvent.event == EventType.CLOSE) {
                    logger.info { "Received close event" }
                    return
                } else {
                    dataSent = write(logger, writeSseEventMeasurement, sseEvent, output, dataSent)
                }
                if (!checkStatus(queue, handler, logger)) {
                    return
                }
            } finally { pseMeasurement.close() }
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
            flush(output)
        } catch (e: Exception) {
            logger.error(e) { "cannot flush the remaining data when processing is finished" }
        }
    }
}

private fun checkStatus(
    queue: ArrayBlockingQueue<Supplier<SseEvent>>,
    handler: AbstractCancelableHandler,
    logger: KLogger
): Boolean {
    if (queue.isEmpty() && !handler.isAlive) {
        logger.info { "Request canceled" }
        return false
    }
    return true
}

private fun write(
    logger: KLogger,
    writeSseEventMeasurement: ChildDataMeasurement,
    sseEvent: SseEvent,
    output: OutputStream,
    dataSent: Int
): Int {
    var dataSent1 = dataSent
    logger.debug {
        "Write event to output: " // FIXME: log data
    }
    val wseMeasurement = writeSseEventMeasurement.start()
    try {
        sseEvent.writeData(output)
        output.write('\n'.code)
    } finally {
        wseMeasurement.close()
    }
    dataSent1++
    return dataSent1
}

private fun flush(output: OutputStream) {
    output.flush()
}

private fun processErrorData(
    sseEvent: SseEvent,
    progressListener: ProgressListener
) {
    if (sseEvent is SseEvent.ErrorData) {
        progressListener.onError(sseEvent)
    }
}

private fun writeHeader(
    writeHeader: Boolean,
    sseEvent: SseEvent,
    status: HttpStatus,
    ctx: Context
): Boolean {
    var status1 = status
    if (writeHeader && sseEvent is SseEvent.ErrorData.SimpleError) {
        // something happened during request
        status1 = HttpStatus.INTERNAL_SERVER_ERROR
    }
    if (writeHeader) {
        ctx.status(status1)
            .contentType(JSON_STREAM_CONTENT_TYPE)
            .header(Header.TRANSFER_ENCODING, "chunked")
    }
    return false
}

private fun awaitConver(
    awaitConvertToJsonMeasurement: ChildDataMeasurement,
    nextEvent: Supplier<SseEvent>
): SseEvent {
    val actjMeasurement = awaitConvertToJsonMeasurement.start()
    val sseEvent = try {
        nextEvent.get()
    } finally {
        actjMeasurement.close()
    }
    return sseEvent
}

private fun updateQueueMetric(
    matchedPath: String,
    queue: ArrayBlockingQueue<Supplier<SseEvent>>
) {
    ResponseQueue.currentSize(matchedPath, queue.size)
}

private fun awaitNextEvent(
    awaitNextMeasurement: ChildDataMeasurement,
    queue: ArrayBlockingQueue<Supplier<SseEvent>>
): Supplier<SseEvent> {
    val anMeasurement = awaitNextMeasurement.start()
    val nextEvent = try {
        queue.take()
    } finally {
        anMeasurement.close()
    }
    return nextEvent
}