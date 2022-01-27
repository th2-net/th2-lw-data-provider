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

package com.exactpro.th2.lwdataprovider.handlers

import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.messages.*
import com.exactpro.th2.lwdataprovider.*
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import io.prometheus.client.Counter
import mu.KotlinLogging
import java.util.concurrent.ThreadPoolExecutor

class SearchMessagesHandler(
    private val cradleMsgExtractor: CradleMessageExtractor,
    private val threadPool: ThreadPoolExecutor
) {
    companion object {
        private val logger = KotlinLogging.logger { }

        private val processedMessageCount = Counter.build(
            "processed_message_count", "Count of processed Message"
        ).register()
    }

    fun extractStreamNames(): Collection<String> {
        logger.info { "Getting stream names" }
        return cradleMsgExtractor.getStreams();
    }
    
    fun loadMessages(request: SseMessageSearchRequest, requestContext: MessageRequestContext) {
        
        if (request.stream == null && request.resumeFromIdsList.isNullOrEmpty()) {
            return;
        }

        threadPool.execute {
            try {

                if (!request.resumeFromIdsList.isNullOrEmpty()) {
                    request.resumeFromIdsList.forEach { resumeFromId ->
                        val filter = StoredMessageFilterBuilder().apply {
                            streamName().isEqualTo(resumeFromId.streamName)
                            direction().isEqualTo(resumeFromId.direction)
                            if (request.searchDirection == AFTER) {
                                index().isGreaterThanOrEqualTo(resumeFromId.index)
                            } else {
                                index().isLessThanOrEqualTo(resumeFromId.index)
                            }

                            request.resultCountLimit?.let { limit(it) }

                        }.build()

                        if (!request.onlyRaw)
                            cradleMsgExtractor.getMessages(filter, requestContext)
                        else
                            cradleMsgExtractor.getRawMessages(filter, requestContext)
                    }
                } else {
                    request.stream?.forEach { stream ->

                        for (direction in Direction.values()) {
                            val filter = StoredMessageFilterBuilder().apply {
                                streamName().isEqualTo(stream)
                                direction().isEqualTo(direction)
                                request.startTimestamp?.let { timestampFrom().isGreaterThanOrEqualTo(it) }
                                request.endTimestamp?.let { timestampTo().isLessThan(it) }
                                request.resultCountLimit?.let { limit(it) }
                            }.build()

                            if (!request.onlyRaw)
                                cradleMsgExtractor.getMessages(filter, requestContext)
                            else
                                cradleMsgExtractor.getRawMessages(filter, requestContext)
                        }
                    }
                }

                requestContext.allDataLoadedFromCradle()
                if (requestContext.requestedMessages.isEmpty()) {
                    requestContext.finishStream()
                }
            } catch (e: Exception) {
                logger.error("Error getting messages", e)
                requestContext.writeErrorMessage(e.message?:"")
                requestContext.finishStream()
            }
        }
    }

    fun loadOneMessage(request: GetMessageRequest, requestContext: MessageRequestContext) {

        threadPool.execute {
            try {
                cradleMsgExtractor.getMessage(StoredMessageId.fromString(request.msgId), request.onlyRaw, requestContext);
                requestContext.allDataLoadedFromCradle()
                if (requestContext.requestedMessages.isEmpty()) {
                    requestContext.finishStream()
                }
            } catch (e: Exception) {
                logger.error("Error getting messages", e)
                requestContext.writeErrorMessage(e.message?:"")
                requestContext.finishStream()
            }
        }
    }
}

    
