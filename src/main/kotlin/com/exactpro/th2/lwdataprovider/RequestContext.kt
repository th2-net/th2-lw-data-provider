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

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

abstract class RequestContext(
   open val channelMessages: ResponseHandler,
   val requestParameters: Map<String, Any> = emptyMap(),
   val counter: AtomicLong = AtomicLong(0L),
   val scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()
) {

   @Volatile var contextAlive: Boolean = true

   companion object {
      private val logger = KotlinLogging.logger { }
   }
   
   fun finishStream() {
      channelMessages.finishStream()
   }

   fun writeErrorMessage(text: String) {
      logger.info { text }
      channelMessages.writeErrorMessage(text)
   }

   fun keepAliveEvent() {
      channelMessages.keepAliveEvent(scannedObjectInfo, counter);
   }
}

abstract class MessageRequestContext (
   channelMessages: ResponseHandler,
   requestParameters: Map<String, Any> = emptyMap(),
   counter: AtomicLong = AtomicLong(0L),
   scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo(),
   val requestedMessages: MutableMap<String, RequestedMessageDetails> = ConcurrentHashMap(),
   val streamInfo: ProviderStreamInfo = ProviderStreamInfo()
) : RequestContext(channelMessages, requestParameters, counter, scannedObjectInfo) {

   val allMessagesRequested: AtomicBoolean = AtomicBoolean(false)
   var loadedMessages = 0

   fun registerMessage(message: RequestedMessageDetails) {
      requestedMessages[message.id] = message
   }

   fun allDataLoadedFromCradle() = allMessagesRequested.set(true)

   abstract fun createMessageDetails(id: String, time: Long, storedMessage: StoredMessage): RequestedMessageDetails;
   abstract fun addStreamInfo();
   
}

abstract class RequestedMessageDetails (
   val id: String,
   @Volatile var time: Long,
   val storedMessage: StoredMessage,
   protected open val context: MessageRequestContext,
   var parsedMessage: List<Message>? = null,
   var rawMessage: RawMessage? = null
) {

   abstract fun responseMessage();
   
   fun notifyMessage() {
      context.apply { 
         val reqDetails = requestedMessages.remove(id)
         scannedObjectInfo.update(id, Instant.now(), counter)
         if (requestedMessages.isEmpty() && allMessagesRequested.get()) {
            addStreamInfo()
            finishStream()
         }
      }
   }
}