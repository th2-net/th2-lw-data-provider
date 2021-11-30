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

package com.exactpro.th2.ldsprovider.http

import com.exactpro.th2.ldsprovider.CustomJsonFormatter
import com.exactpro.th2.ldsprovider.MessageRequestContext
import com.exactpro.th2.ldsprovider.workers.KeepAliveHandler
import com.exactpro.th2.ldsprovider.SseEvent
import com.exactpro.th2.ldsprovider.SseResponseBuilder
import com.exactpro.th2.ldsprovider.configuration.Configuration
import com.exactpro.th2.ldsprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.ldsprovider.handlers.SearchMessagesHandler
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetMessagesServlet (
    private val configuration: Configuration, private val jacksonMapper: ObjectMapper, 
    private val keepAliveHandler: KeepAliveHandler, 
    private val searchMessagesHandler: SearchMessagesHandler
    )
    : SseServlet() {

    private val customJsonFormatter = CustomJsonFormatter()

    companion object {
        private val logger = KotlinLogging.logger { }
    }
    
    override fun doGet(req: HttpServletRequest?, resp: HttpServletResponse?) {
        
        checkNotNull(req)
        checkNotNull(resp)
        
        val queryParametersMap = getParameters(req)
        logger.info { "Received search sse event request with parameters: $queryParametersMap" }
        
        val request = SseMessageSearchRequest(queryParametersMap)
        request.checkRequest()
        
        val queue = ArrayBlockingQueue<SseEvent>(configuration.responseQueueSize)
        val sseResponseBuilder = SseResponseBuilder(jacksonMapper)
        val reqContext = MessageRequestContext(sseResponseBuilder, queryParametersMap, channelMessages = queue,
            jsonFormatter = customJsonFormatter)
        keepAliveHandler.addKeepAliveData(reqContext)
        searchMessagesHandler.loadMessages(request, reqContext)

        this.waitAndWrite(queue, resp)
        keepAliveHandler.removeKeepAliveData(reqContext)
        logger.info { "Processing search sse messages request finished" }
    }
    

}