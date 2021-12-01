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

package com.exactpro.th2.lwdataprovider.entities.filters.events

import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.filters.Filter
import com.exactpro.th2.lwdataprovider.entities.filters.FilterRequest
import com.exactpro.th2.lwdataprovider.entities.filters.info.FilterInfo
import com.exactpro.th2.lwdataprovider.entities.filters.info.FilterParameterType
import com.exactpro.th2.lwdataprovider.entities.filters.info.FilterSpecialType.NEED_BODY
import com.exactpro.th2.lwdataprovider.entities.filters.info.Parameter
import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity

class EventBodyFilter private constructor(
    private var body: List<String>, override var negative: Boolean = false, override var conjunct: Boolean = false
) : Filter<BaseEventEntity> {
    companion object {

        fun build(filterRequest: FilterRequest): Filter<BaseEventEntity> {
            return EventBodyFilter(
                negative = filterRequest.isNegative(),
                conjunct = filterRequest.isConjunct(),
                body = filterRequest.getValues()
                    ?: throw InvalidRequestException("'${filterInfo.name}-values' cannot be empty")
            )
        }

        val filterInfo = FilterInfo(
            "body",
            "matches events whose body contains one of the specified tokens",
            mutableListOf<Parameter>().apply {
                add(Parameter("negative", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("conjunct", FilterParameterType.BOOLEAN, false, null))
                add(Parameter("values", FilterParameterType.STRING_LIST, null, "FGW, ..."))
            },
            NEED_BODY
        )
    }

    override fun match(element: BaseEventEntity): Boolean {
        val predicate: (String) -> Boolean = { item ->
            element.body?.toLowerCase()?.contains(item.toLowerCase()) ?: false
        }
        return negative.xor(if (conjunct) body.all(predicate) else body.any(predicate))
    }

    override fun getInfo(): FilterInfo {
        return filterInfo
    }

}
