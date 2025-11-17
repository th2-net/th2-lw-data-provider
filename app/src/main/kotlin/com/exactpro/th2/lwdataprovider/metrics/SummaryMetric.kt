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
import io.prometheus.client.Summary

class SummaryMetric private constructor(
    name: String,
    registry: CollectorRegistry,
) : Metric() {
    private val summary = Summary.build(
        "th2_ldp_${name.replace(' ', '_').lowercase()}_time", "Time spent on each action for $name"
    ).labelNames("action")
        .register(registry)

    override fun child(name: String): ChildMetric = SummaryChildMetric(summary.labels(name))
    override fun observe(name: String, amt: Double) = summary.labels(name).observe(amt)

    companion object {
        @JvmStatic
        fun create(registry: CollectorRegistry, name: String): Metric = SummaryMetric(name, registry)

        private class SummaryChildMetric(
            val child: Summary.Child,
        ): ChildMetric() {
            override fun observe(amt: Double) = child.observe(amt)
        }
    }
}