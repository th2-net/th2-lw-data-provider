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
import com.exactpro.th2.lwdataprovider.metrics.Metric
import com.exactpro.th2.lwdataprovider.handlers.AbstractCancelableHandler
import com.exactpro.th2.lwdataprovider.http.listener.ProgressListener
import com.exactpro.th2.lwdataprovider.metrics.HttpWriteMetrics
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
    metric: Metric,
    logger: KLogger,
    progressListener: ProgressListener,
    bufferSize: Int
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
        do {
            val nextEvent = awaitNextEvent(queue)
            val sseEvent = awaitConver(nextEvent)
            writeHeader = writeHeader(writeHeader, sseEvent, status, ctx)
            processErrorData(sseEvent, progressListener)
            if (sseEvent.event == EventType.KEEP_ALIVE) {
                flush(output)
            } else if (sseEvent.event == EventType.CLOSE) {
                logger.info { "Received close event" }
                return
            } else {
                dataSent = write(logger, sseEvent, output, dataSent)
            }
            if (!checkStatus(queue, handler, logger)) {
                return
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
    sseEvent: SseEvent,
    output: OutputStream,
    dataSent: Int
): Int {
    logger.debug {
        "Write event to output: " // FIXME: log data
    }
    sseEvent.writeData(output)
    output.write('\n'.code)
    return dataSent + 1
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
    nextEvent: Supplier<SseEvent>
): SseEvent {
    return nextEvent.get()
}

private fun awaitNextEvent(
    queue: ArrayBlockingQueue<Supplier<SseEvent>>
): Supplier<SseEvent> {
    return queue.take()
}