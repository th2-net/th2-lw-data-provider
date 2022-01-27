/*******************************************************************************
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.StreamResponse
import com.exactpro.th2.dataprovider.grpc.StringList
import com.exactpro.th2.lwdataprovider.*
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.google.protobuf.Empty
import io.grpc.stub.StreamObserver
import java.util.concurrent.ArrayBlockingQueue

open class GrpcDataProviderImpl(
    private val configuration: Configuration,
    private val searchMessagesHandler: SearchMessagesHandler
): DataProviderGrpc.DataProviderImplBase() {

    override fun getMessageStreams(request: Empty?, responseObserver: StreamObserver<StringList>?) {
        val grpcResponse = StringList.newBuilder().addAllListString(searchMessagesHandler.extractStreamNames()).build()
        responseObserver?.apply {
            onNext(grpcResponse)
            onCompleted()
        }
    }

    override fun searchMessages(request: MessageSearchRequest?, responseObserver: StreamObserver<StreamResponse>?) {

        checkNotNull(request)
        checkNotNull(responseObserver)

        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)

        val requestParams = SseMessageSearchRequest(request)
        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcMessageRequestContext(grpcResponseHandler)
        searchMessagesHandler.loadMessages(requestParams, context)
        processResponse(responseObserver, grpcResponseHandler)

    }

    protected open fun processResponse(responseObserver: StreamObserver<StreamResponse>, grpcResponseHandler: GrpcResponseHandler) {
        val buffer = grpcResponseHandler.buffer
        var inProcess = true
        while (inProcess) {
            val event = buffer.take()
            if (event.close) {
                responseObserver.onCompleted()
                inProcess = false
            } else if (event.error != null) {
                responseObserver.onError(event.error)
                inProcess = false
            } else if (event.resp != null) {
                responseObserver.onNext(event.resp)
            }
        }
    }
}