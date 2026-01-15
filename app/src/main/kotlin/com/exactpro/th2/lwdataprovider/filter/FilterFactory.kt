/*
 * Copyright 2022-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.filter

import io.javalin.openapi.Nullability
import io.javalin.openapi.OpenApiPropertyType

interface FilterFactory<T> {
    fun create(requests: Collection<FilterRequest>): DataFilter<T>
}

data class FilterRequest(
    val name: String,
    val values: Collection<String>,
    @get:OpenApiPropertyType(definedBy = Boolean::class, nullability = Nullability.NULLABLE)
    val negative: Boolean = false,
    @get:OpenApiPropertyType(definedBy = Boolean::class, nullability = Nullability.NULLABLE)
    val conjunct: Boolean = false,
    @get:OpenApiPropertyType(definedBy = FilterOperator::class, nullability = Nullability.NULLABLE)
    val operator: FilterOperator = FilterOperator.EQUAL
)

@Suppress("unused")
enum class FilterOperator(
    val predicate: (value: String, other: String) -> Boolean,
) {
    EQUAL ({ value, other -> value.equals(other, ignoreCase = true) }),
    CONTAIN ({ value, other -> value.contains(other, ignoreCase = true) }),
    START_WITH ({ value, other -> value.startsWith(other, ignoreCase = true) }),
    END_WITH ({ value, other -> value.endsWith(other, ignoreCase = true) }),
}