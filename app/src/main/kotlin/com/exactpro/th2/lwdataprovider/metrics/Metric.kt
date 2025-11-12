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

package com.exactpro.th2.lwdataprovider.metrics

import io.prometheus.client.SimpleTimer
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

@OptIn(ExperimentalContracts::class)
abstract class Metric {
    inline fun <T> measure(name: String, block: () -> T): T {
        contract {
            callsInPlace(block, InvocationKind.EXACTLY_ONCE)
        }
        val startNanos = System.nanoTime()
        try {
            return block()
        } finally {
            observe(name, SimpleTimer.elapsedSecondsFromNanos(startNanos, System.nanoTime()))
        }
    }
    abstract fun child(name: String): ChildMetric
    abstract fun observe(name: String, amt: Double)
}

@OptIn(ExperimentalContracts::class)
abstract class ChildMetric {
    inline fun <T> measure(block: () -> T): T {
        contract {
            callsInPlace(block, InvocationKind.EXACTLY_ONCE)
        }
        val startNanos = System.nanoTime()
        try {
            return block()
        } finally {
            observe(SimpleTimer.elapsedSecondsFromNanos(startNanos, System.nanoTime()))
        }
    }
    abstract fun observe(amt: Double)
}