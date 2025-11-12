/*
 * Copyright 2023-2025 Exactpro (Exactpro Systems Limited)
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

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter

class ImplMetric private constructor(
    name: String,
    registry: CollectorRegistry,
) : Metric() {

    private val counter = Counter.build(
        "th2_ldp_${name.replace(' ', '_').lowercase()}_time_count", "Number of measurement on each action for $name"
    ).labelNames("action")
        .register(registry)

    private val sum = Counter.build(
        "th2_ldp_${name.replace(' ', '_').lowercase()}_time_sum", "Sum time of measurement on each action for $name"
    ).labelNames("action")
        .register(registry)

    override fun child(name: String): ChildMetric =
        ImplChildMetric(counter.labels(name), sum.labels(name))

    override fun observe(name: String, amt: Double) {
        counter.labels(name).inc()
        sum.labels(name).inc(amt)
    }

    companion object {

        @JvmStatic
        fun create(registry: CollectorRegistry, name: String): Metric =
            ImplMetric(name, registry)

        private class ImplChildMetric(
            private val counter: Counter.Child,
            private val sum: Counter.Child,
        ): ChildMetric() {
            override fun observe(amt: Double) {
                counter.inc()
                sum.inc(amt)
            }
        }
    }
}

