/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.entities.responses.ser

import java.lang.AutoCloseable
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

interface ByteBufferPool {
    fun acquire(size: Int): ByteBuffer
    fun release(buffer: ByteBuffer)
}

object DummyBufferPool : ByteBufferPool {
    override fun acquire(size: Int): ByteBuffer = ByteBuffer.allocate(size)

    override fun release(buffer: ByteBuffer) {}
}

class HeapBufferPool: ByteBufferPool, AutoCloseable {
    private val pool = ConcurrentLinkedQueue<ByteBuffer>()

    override fun acquire(size: Int): ByteBuffer {
        return pool.poll()?.let {
            if (it.limit() < size) {
                ByteBuffer.allocate(size)
            } else {
                it.clear()
            }
        } ?: ByteBuffer.allocate(size)
    }

    override fun release(buffer: ByteBuffer) {
        pool.offer(buffer.clear())
    }

    override fun close() {
        pool.clear()
    }
}