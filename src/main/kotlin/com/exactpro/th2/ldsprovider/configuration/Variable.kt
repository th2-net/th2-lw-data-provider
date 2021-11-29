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

package com.exactpro.th2.ldsprovider.configuration

import mu.KotlinLogging

class VariableBuilder {
    
    companion object {
        private val logger = KotlinLogging.logger { }
        
        private fun <T> printToLog(name: String, value: T, isDefault: Boolean, showInLog: Boolean,
                                   converter : (T) -> String = { it.toString() }) {
            logger.info {
                val valueToLog = if (showInLog) converter.invoke(value) else "*****"

                if (isDefault)
                    "property '$name' is not set - defaulting to '$valueToLog'"
                else
                    "property '$name' is set to '$valueToLog'"
            }
        }
        
        fun <T> getVariable(name: String, param: T?, defaultValue: T, showInLog: Boolean = true) : T {
            val value = param ?: defaultValue
            printToLog(name, value as Any, param == null, showInLog)
            return value
        }
        
        
    }
}