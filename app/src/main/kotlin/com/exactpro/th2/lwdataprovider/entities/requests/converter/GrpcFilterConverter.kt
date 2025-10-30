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

package com.exactpro.th2.lwdataprovider.entities.requests.converter

import com.exactpro.th2.dataprovider.lw.grpc.Filter
import com.exactpro.th2.dataprovider.lw.grpc.FilterOperator.CONTAIN
import com.exactpro.th2.dataprovider.lw.grpc.FilterOperator.END_WITH
import com.exactpro.th2.dataprovider.lw.grpc.FilterOperator.EQUAL
import com.exactpro.th2.dataprovider.lw.grpc.FilterOperator.START_WITH
import com.exactpro.th2.dataprovider.lw.grpc.FilterOperator.UNRECOGNIZED
import com.exactpro.th2.lwdataprovider.filter.FilterOperator
import com.exactpro.th2.lwdataprovider.filter.FilterRequest

object GrpcFilterConverter {
    fun convert(filters: Collection<Filter>): Collection<FilterRequest> {
        return filters.map {
            FilterRequest(
                it.name.name,
                it.valueList,
                it.negative,
                it.conjunct,
                when(it.operator) {
                    EQUAL -> FilterOperator.EQUAL
                    CONTAIN -> FilterOperator.CONTAIN
                    START_WITH -> FilterOperator.START_WITH
                    END_WITH -> FilterOperator.END_WITH
                    UNRECOGNIZED -> FilterOperator.EQUAL
                },
            )
        }
    }
}