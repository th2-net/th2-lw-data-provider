/*
 * Copyright 2022-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.filter.events

import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.th2.lwdataprovider.filter.DataFilter
import com.exactpro.th2.lwdataprovider.filter.FilterFactory
import com.exactpro.th2.lwdataprovider.filter.FilterRequest
import com.exactpro.th2.lwdataprovider.filter.JoinedDataFilter
import com.exactpro.th2.lwdataprovider.filter.events.impl.EventSimpleFilter

object EventsFilterFactory : FilterFactory<StoredTestEvent> {
    override fun create(requests: Collection<FilterRequest>): DataFilter<StoredTestEvent> =
        if (requests.isEmpty()) {
            DataFilter.acceptAll()
        } else {
            JoinedDataFilter(requests.toFilters())
        }
}

private fun Collection<FilterRequest>.toFilters(): Collection<DataFilter<StoredTestEvent>> = map {
    when (it.name) {
        "type" -> it.toSimpleFilter { type }
        "name" -> it.toSimpleFilter { name }
        else -> error("unsupported filter ${it.name}")
    }
}

private fun FilterRequest.toSimpleFilter(accessor: StoredTestEvent.() -> String): DataFilter<StoredTestEvent> =
    EventSimpleFilter(values, negative, conjunct, accessor)