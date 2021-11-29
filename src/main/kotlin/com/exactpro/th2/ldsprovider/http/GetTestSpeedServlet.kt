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

import io.ktor.http.HttpHeaders
import org.eclipse.jetty.http.HttpStatus
import java.util.Random
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetTestSpeedServlet() : HttpServlet() {
    
    private var str : ByteArray = ByteArray(1024 * 1024);


    init {
        Random().nextBytes(str)
    }

    override fun doGet(req: HttpServletRequest?, resp: HttpServletResponse?) {

        resp!!.contentType = "text/event-stream"
        resp.status = HttpStatus.OK_200
        resp.addHeader(HttpHeaders.CacheControl, "no-cache, no-store")
        resp.outputStream.use {
            for (i in 1..1024)
                it.write(str, 0, str.size)
        }
        
    }
}