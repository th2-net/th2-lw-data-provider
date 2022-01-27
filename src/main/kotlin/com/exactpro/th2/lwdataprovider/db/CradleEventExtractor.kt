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

package com.exactpro.th2.lwdataprovider.db

/*
 * INFO: Event processing are disabled in cradle-api 2.+ version
 * Event processing is in 3+ version
 */


import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.http.SseEventRequestContext
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalDateTime


class CradleEventExtractor (private val cradleManager: CradleManager) {

    private val storage = cradleManager.storage

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    fun getEvents(filter: SseEventSearchRequest, requestContext: SseEventRequestContext) {
        TODO("NOT IMPLEMENTED FOR CRADLE API 2.+")
    }

    fun getSingleEvents(filter: GetEventRequest, requestContext: SseEventRequestContext) {
        TODO("NOT IMPLEMENTED FOR CRADLE API 2.+")
    }

    private fun toLocal(timestamp: Instant?): LocalDateTime {
        TODO("NOT IMPLEMENTED FOR CRADLE API 2.+")
    }

    private fun toInstant(timestamp: LocalDateTime): Instant {
        TODO("NOT IMPLEMENTED FOR CRADLE API 2.+")
    }


    private fun splitByDates(from: Instant?, to: Instant?): Collection<Pair<Instant, Instant>> {
        TODO("NOT IMPLEMENTED FOR CRADLE API 2.+")
    }

    private fun getEventByDates(dates: Collection<Pair<Instant, Instant>>, requestContext: SseEventRequestContext) {
        TODO("NOT IMPLEMENTED FOR CRADLE API 2.+")
    }

    private fun getEventByIds(id: ProviderEventId, dates: Collection<Pair<Instant, Instant>>, requestContext: SseEventRequestContext) {
        TODO("NOT IMPLEMENTED FOR CRADLE API 2.+")
    }

    private fun loadAttachedMessages(messageIds: Collection<StoredMessageId>?): Set<String> {
        TODO("NOT IMPLEMENTED FOR CRADLE API 2.+")
    }

    private fun processEvents(
        testEvents: Iterable<StoredTestEventWrapper>,
        requestContext: SseEventRequestContext, count: LongCounter
    ) {
        TODO("NOT IMPLEMENTED FOR CRADLE API 2.+")
    }
}

class LongCounter {
    var value: Long = 0;

    override fun toString(): String {
        return value.toString()
    }
}