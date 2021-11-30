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

package com.exactpro.th2.ldsprovider

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.ldsprovider.entities.responses.Event
import com.exactpro.th2.ldsprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.ldsprovider.producers.MessageProducer53
import com.google.gson.Gson
import mu.KotlinLogging
import java.time.Instant
import java.util.Collections
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

abstract class RequestContext(
   val responseBuilder: SseResponseBuilder,
   val requestParameters: Map<String, Any> = emptyMap(),
   val counter: AtomicLong = AtomicLong(0L),
   val channelMessages: ArrayBlockingQueue<SseEvent>,
   val scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()
) {

   companion object {
      private val logger = KotlinLogging.logger { }
   }
   
   fun finishStream() {
      channelMessages.put(SseEvent(event = EventType.CLOSE))
   }
   
   fun keepAliveEvent() {
      channelMessages.put(responseBuilder.build(scannedObjectInfo, counter))
   }
   
   fun writeErrorMessage(text: String) {
      logger.info { text }
      channelMessages.put(SseEvent(Gson().toJson(Collections.singletonMap("message", text)), EventType.ERROR))
   }
}

class MessageRequestContext (
   responseBuilder: SseResponseBuilder,
   requestParameters: Map<String, Any> = emptyMap(),
   counter: AtomicLong = AtomicLong(0L),
   channelMessages: ArrayBlockingQueue<SseEvent>,
   scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo(),
   val requestedMessages: MutableMap<String, RequestedMessageDetails> = ConcurrentHashMap(),
   val allMessagesRequested: AtomicBoolean = AtomicBoolean(false),
   val jsonFormatter: CustomJsonFormatter = CustomJsonFormatter()
) : RequestContext(responseBuilder, requestParameters, counter, channelMessages, scannedObjectInfo) {

   fun registerMessage(message: RequestedMessageDetails) {
      requestedMessages[message.id] = message
   }

   fun allDataLoadedFromCradle() = allMessagesRequested.set(true)
   
}

class EventRequestContext (
   responseBuilder: SseResponseBuilder,
   requestParameters: Map<String, Any> = emptyMap(),
   counter: AtomicLong = AtomicLong(0L),
   channelMessages: ArrayBlockingQueue<SseEvent>,
   scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()
) : RequestContext(responseBuilder, requestParameters, counter, channelMessages, scannedObjectInfo) {

   fun processEvent(event: Event) {
      val sseEvent = responseBuilder.build(event, counter)
      channelMessages.put(sseEvent)
      scannedObjectInfo.update(event.eventId, System.currentTimeMillis(), counter)
   }

}

class RequestedMessageDetails (
   val id: String,
   @Volatile var time: Long,
   val storedMessage: StoredMessage,
   private val context: MessageRequestContext,
   var parsedMessage: List<Message>? = null,
   var rawMessage: RawMessage? = null
) {

   fun responseMessage53() {
      val msg = MessageProducer53.createMessage(this, context.jsonFormatter)
      val event = context.responseBuilder.build(msg, this.context.counter)
      context.channelMessages.put(event)
   }
   
   fun notifyMessage() {
      context.apply { 
         val reqDetails = requestedMessages.remove(id)
         scannedObjectInfo.update(id, Instant.now(), counter)
         if (requestedMessages.isEmpty() && allMessagesRequested.get()) {
            finishStream()
         }
      }
   }
}