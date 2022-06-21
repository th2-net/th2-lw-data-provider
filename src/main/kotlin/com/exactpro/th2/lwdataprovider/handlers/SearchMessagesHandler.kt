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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.BookInfo
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.MessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import mu.KotlinLogging
import java.util.concurrent.ExecutorService
import kotlin.math.max

class SearchMessagesHandler(
    private val cradleMsgExtractor: CradleMessageExtractor,
    private val threadPool: ExecutorService
) {
    companion object {
        private val logger = KotlinLogging.logger { }
    }

    fun extractBookNames(): Collection<BookInfo> {
        return cradleMsgExtractor.getBooks()
    }

    fun extractStreamNames(bookId: BookId): Collection<String> {
        logger.info { "Getting stream names" }
        return cradleMsgExtractor.getStreams(bookId);
    }
    
    fun loadMessages(request: SseMessageSearchRequest, requestContext: MessageRequestContext) {
        
        if (request.stream == null && request.resumeFromIdsList.isNullOrEmpty()) {
            return;
        }

        threadPool.execute {
            try {

                var limitReached = false;
                if (!request.resumeFromIdsList.isNullOrEmpty()) {
                    request.resumeFromIdsList.forEach { resumeFromId ->
                        requestContext.streamInfo.registerSession(resumeFromId.sessionAlias, resumeFromId.direction)
                        if (limitReached)
                            return@forEach;
                        if (!requestContext.contextAlive)
                            return@execute;
                        val filter = MessageFilterBuilder().apply {
                            bookId(request.bookId)
                            sessionAlias(resumeFromId.sessionAlias)
                            direction(resumeFromId.direction)
                            if (request.searchDirection == AFTER) {
                                sequence().isGreaterThanOrEqualTo(resumeFromId.sequence)
                            } else {
                                sequence().isLessThanOrEqualTo(resumeFromId.sequence)
                            }

                            request.startTimestamp?.let { timestampFrom().isGreaterThanOrEqualTo(it) }
                            request.endTimestamp?.let { timestampTo().isLessThan(it) }
                            request.resultCountLimit?.let { limit(max(it - requestContext.loadedMessages, 0)) }

                        }.build()

                        if (!request.onlyRaw)
                            cradleMsgExtractor.getMessages(filter, requestContext)
                        else
                            cradleMsgExtractor.getRawMessages(filter, requestContext)
                        limitReached = request.resultCountLimit != null && request.resultCountLimit <= requestContext.loadedMessages
                    }
                } else {
                    request.stream?.forEach { (stream, direction) ->
                        requestContext.streamInfo.registerSession(stream, direction)
                        if (limitReached)
                            return@forEach;
                        if (!requestContext.contextAlive)
                            return@execute;

                        val filter = MessageFilterBuilder().apply {
                            bookId(bookId)
                            sessionAlias(stream)
                            direction(direction)
                            request.startTimestamp?.let { timestampFrom().isGreaterThanOrEqualTo(it) }
                            request.endTimestamp?.let { timestampTo().isLessThan(it) }
                            request.resultCountLimit?.let { limit(max(it - requestContext.loadedMessages, 0)) }
                        }.build()

                        if (!request.onlyRaw)
                            cradleMsgExtractor.getMessages(filter, requestContext)
                        else
                            cradleMsgExtractor.getRawMessages(filter, requestContext)

                        limitReached = request.resultCountLimit != null && request.resultCountLimit <= requestContext.loadedMessages
                    }
                }

                requestContext.allDataLoadedFromCradle()
                if (requestContext.requestedMessages.isEmpty()) {
                    requestContext.addStreamInfo()
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
                cradleMsgExtractor.getMessage(request.msgId, request.onlyRaw, requestContext);
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

    fun loadMessageGroups(request: MessagesGroupRequest, requestContext: MessageRequestContext) {
        if (request.groups.isEmpty()) {
            requestContext.finishStream()
        }

        threadPool.execute {
            try {
                request.groups.forEach { group ->
                    logger.debug { "Executing request for group $group" }
                    cradleMsgExtractor.getMessagesGroup(GroupedMessageFilter.builder()
                        .groupName(group)
                        .bookId(request.bookId)
                        .timestampFrom().isGreaterThanOrEqualTo(request.startTimestamp)
                        .timestampTo().isLessThan(request.endTimestamp)
                        .build(), request.sort, request.rawOnly, requestContext)
                    logger.debug { "Executing of request for group $group has been finished" }
                }

                requestContext.allDataLoadedFromCradle()
                if (requestContext.requestedMessages.isEmpty()) {
                    requestContext.addStreamInfo()
                    requestContext.finishStream()
                }
            } catch (ex: Exception) {
                logger.error("Error getting messages group", ex)
                requestContext.writeErrorMessage(ex.message ?: "")
                requestContext.finishStream()
            }
        }
    }
}

    
