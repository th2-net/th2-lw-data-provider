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

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.lang.AutoCloseable
import java.util.concurrent.ConcurrentLinkedQueue

interface ByteBufPool {
    fun acquire(size: Int): ByteBuf
    fun release(buf: ByteBuf)
}

object DummyBufPool : ByteBufPool {
    override fun acquire(size: Int): ByteBuf = Unpooled.buffer(size)

    override fun release(buf: ByteBuf) {}
}

class UnpooledBufPool(
    private val bufferSize: Int = 1_024 * 2,
) : ByteBufPool, AutoCloseable {
    private val pool = ConcurrentLinkedQueue<ByteBuf>()

    override fun acquire(size: Int): ByteBuf {
        return pool.poll()?.let {
            it.clear().apply {
                if (it.capacity() < size) { it.ensureWritable(size) }
            }
        } ?: Unpooled.buffer(bufferSize)
    }

    override fun release(buf: ByteBuf) {
        if (!pool.offer(buf.clear())) {
            buf.release()
        }
    }

    override fun close() {
        while (pool.isNotEmpty()) {
            acquire(0).release()
        }
    }
}