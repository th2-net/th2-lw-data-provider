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

import com.exactpro.th2.dataprovider.grpc.StreamResponse
import com.exactpro.th2.lwdataprovider.GrpcResponseHandler
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.grpc.stub.ServerCallStreamObserver
import io.grpc.stub.StreamObserver
import mu.KotlinLogging

class GrpcDataProviderBackPressure(configuration: Configuration, searchMessagesHandler: SearchMessagesHandler) :
    GrpcDataProviderImpl(configuration, searchMessagesHandler) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    override fun processResponse(
        responseObserver: StreamObserver<StreamResponse>,
        grpcResponseHandler: GrpcResponseHandler
    ) {
        val servCallObs = responseObserver as ServerCallStreamObserver<StreamResponse>
        servCallObs.setOnReadyHandler {
            if (grpcResponseHandler.streamClosed)
                return@setOnReadyHandler;
            val buffer = grpcResponseHandler.buffer
            var inProcess = true
            while (servCallObs.isReady && inProcess) {
                val event = buffer.take()
                if (event.close) {
                    servCallObs.onCompleted()
                    inProcess = false
                    grpcResponseHandler.streamClosed = true
                    logger.info { "Executing finished successfully" }
                } else if (event.error != null) {
                    servCallObs.onError(event.error)
                    inProcess = false
                    grpcResponseHandler.streamClosed = true
                    logger.warn(event.error) { "Executing finished with error" }
                } else if (event.resp != null) {
                    servCallObs.onNext(event.resp)
                }
            }
        }

        servCallObs.setOnCancelHandler {
            logger.warn{ "Execution cancelled" }
            grpcResponseHandler.streamClosed = true
            val buffer = grpcResponseHandler.buffer
            while (buffer.poll() != null);
        }


    }
}