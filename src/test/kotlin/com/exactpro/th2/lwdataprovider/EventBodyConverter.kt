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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity
import org.testng.Assert
import org.testng.annotations.Test

class EventBodyConverter {
    
    @Test
    fun test1 () {
        val from:String? = ""
        val toExp:String = "{}"

        Assert.assertEquals(BaseEventEntity.checkAndConvertBody(from), toExp)
    }

    @Test
    fun test2 () {
        val from:String? = null
        val toExp:String = "{}"

        Assert.assertEquals(BaseEventEntity.checkAndConvertBody(from), toExp)
    }

    @Test
    fun test3 () {
        val from:String? = "{'abc': 'abc'}"
        val toExp:String = from!!

        Assert.assertEquals(BaseEventEntity.checkAndConvertBody(from), toExp)
    }

    @Test
    fun test4 () {
        val from:String? = "[{'abc': 'abc'}, {'abc': 'abc'}]"
        val toExp:String = from!!

        Assert.assertEquals(BaseEventEntity.checkAndConvertBody(from), toExp)
    }

    @Test
    fun test5 () {
        val from:String? = "abd33422"
        val toExp:String = "\"" + from!! + "\""

        Assert.assertEquals(BaseEventEntity.checkAndConvertBody(from), toExp)
    }

    @Test
    fun test6 () {
        val from:String? = "abd\"33422"
        val toExp:String = "\"abd\\\"33422\""

        Assert.assertEquals(BaseEventEntity.checkAndConvertBody(from), toExp)
    }

    @Test
    fun test7 () {
        val from:String? = "]"
        val toExp:String = "\"]\""

        Assert.assertEquals(BaseEventEntity.checkAndConvertBody(from), toExp)
    }


}