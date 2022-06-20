/*
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
 */

package com.exactpro.th2.lwdataprovider.integration

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import java.nio.file.Path

class ProviderContainer : GenericContainer<ProviderContainer>(
    ImageFromDockerfile("testcontainers/lw-provider")
        .withFileFromPath(".", Path.of("build"))
        .withDockerfileFromBuilder {
            with(it) {
                from("adoptopenjdk/openjdk11:alpine")
                workDir("/home")
                copy("docker", ".")
                entryPoint("/home/service/bin/service", "run", "com.exactpro.th2.lwdataprovider.MainKt")
            }
        }
) {
    init {
        withExposedPorts(DEFAULT_OUT_PORT)
        waitingFor(Wait.forLogMessage(".*Wait shutdown.*\\n", 1))
        withEnv("JAVA_OPTS", "-Ddatastax-java-driver.advanced.connection.init-query-timeout=\"5000 milliseconds\"")
    }

    companion object {
        const val DEFAULT_OUT_PORT = 8080
    }
}