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

import com.exactpro.cradle.BookToAdd
import com.exactpro.cradle.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.cradle.CradleConfidentialConfiguration
import com.exactpro.th2.common.schema.cradle.CradleNonConfidentialConfiguration
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServerConfiguration
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.dataprovider.grpc.BookId
import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageStream
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.grpc.toInstant
import com.exactpro.th2.lwdataprovider.integration.annotation.IntegrationTest
import com.exactpro.th2.lwdataprovider.utils.createMessageToStore
import com.exactpro.th2.lwdataprovider.utils.generateMessageSequence
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.protobuf.BoolValue
import io.grpc.Context
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.images.builder.Transferable
import org.testcontainers.lifecycle.Startable
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.math.min

private val LOGGER = KotlinLogging.logger { }

private const val KEYSPACE: String = "test_lw_provider"

@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class TestLwProviderIntegration {
    private val configurationPath = Path.of("/var/th2/config/")
    private val mapper = jacksonObjectMapper()
    private val network = Network.newNetwork()
    private val cassandra = DefaultCassandraContainer()
        .withNetwork(network)
        .withNetworkAliases("cassandra")
        .withLogConsumer(Slf4jLogConsumer(LoggerFactory.getLogger("cassandra")))
    private val rabbit = RabbitMQContainer(DockerImageName.parse("rabbitmq:3.8-management-alpine"))
        .withNetwork(network)
        .withNetworkAliases("rabbit")
        .withLogConsumer(Slf4jLogConsumer(LoggerFactory.getLogger("rabbitmq")))
    private val provider = ProviderContainer()
        .withNetwork(network)
        .withLogConsumer(Slf4jLogConsumer(LoggerFactory.getLogger("provider")))

    private lateinit var localCommonFactory: CommonFactory
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private lateinit var client: DataProviderGrpc.DataProviderBlockingStub

    @BeforeAll
    fun beforeAll(@TempDir tmp: Path) {
        rabbit.initQueues(
            mapOf(
                "codec-in" to listOf("codec-request"),
                "codec-response" to listOf("codec-out"),
                "estore" to listOf("estore"),
            )
        )
        startAll(cassandra, rabbit)

        with(provider) {
            withCopyToContainer(
                cassandra.createConfig(keyspace = KEYSPACE).asTransferable(),
                configurationPath.resolve(CRADLE_CONFIDENTIAL_FILE_NAME).toString(),
            )
            withCopyToContainer(
                CradleNonConfidentialConfiguration(
                    prepareStorage = true,
                ).asTransferable(),
                configurationPath.resolve(CRADLE_NON_CONFIDENTIAL_FILE_NAME).toString(),
            )
            withCopyToContainer(
                rabbit.createConfig().asTransferable(),
                configurationPath.resolve(RABBIT_MQ_FILE_NAME).toString(),
            )
            withCopyToContainer(
                Transferable.of(
                    """
                    log4j.rootLogger=INFO, stdout
                    log4j.category.com.exactpro=DEBUG
                    log4j.appender.stdout.Threshold=DEBUG
                    log4j.appender.stdout=org.apache.log4j.ConsoleAppender
                    log4j.appender.stdout.Target=System.out
                    log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
                    log4j.appender.stdout.layout.ConversionPattern=[%d{yyyy-MM-dd|HH:mm:ss.SSS}|%p|%F:%L]: %m%n
                    """.trimIndent()
                ),
                configurationPath.resolve("log4j.properties").toString(),
            )

            withCopyToContainer(
                MessageRouterConfiguration(
                    queues = mapOf(
                        "estore" to QueueConfiguration(
                            routingKey = "estore",
                            queue = "",
                            exchange = RABBIT_MQ_EXCHANGE,
                            attributes = listOf(QueueAttribute.PUBLISH.value, QueueAttribute.EVENT.value)
                        ),
                        "codec-send" to QueueConfiguration(
                            routingKey = "codec-request",
                            queue = "",
                            exchange = RABBIT_MQ_EXCHANGE,
                            attributes = listOf(QueueAttribute.PUBLISH.value, QueueAttribute.RAW.value, "test-group") + (0 until 5).map { "test-$it" } + listOf(
                                "test-alias-0"
                            ),
                        ),
                        "codec-receive" to QueueConfiguration(
                            routingKey = "",
                            queue = "codec-response",
                            exchange = RABBIT_MQ_EXCHANGE,
                            attributes = listOf(QueueAttribute.SUBSCRIBE.value, QueueAttribute.PARSED.value, "from_codec"),
                        )
                    )
                ).asTransferable(),
                configurationPath.resolve(ROUTER_MQ_FILE_NAME).toString(),
            )

            withCopyToContainer(
                GrpcConfiguration(
                    serverConfiguration = GrpcServerConfiguration(
                        host = null, // required to work properly in the container
                        port = ProviderContainer.DEFAULT_OUT_PORT,
                        workers = 5,
                    )
                ).asTransferable(),
                configurationPath.resolve(GRPC_FILE_NAME).toString(),
            )
            withCopyToContainer(
                GrpcRouterConfiguration(workers = 6).asTransferable(),
                configurationPath.resolve(ROUTER_GRPC_FILE_NAME).toString(),
            )

            withCopyToContainer(
                CustomConfigurationClass(
                    hostname = "0.0.0.0",
                    port = 8080,
                    mode = "GRPC",
                ).let(::Configuration).asTransferable(),
                configurationPath.resolve(CUSTOM_FILE_NAME).toString(),
            )

            start()
        }

        localCommonFactory = initCommonFactory(tmp, KEYSPACE)
        initCodecMock(localCommonFactory)
    }

    private fun initCommonFactory(tmp: Path, keyspace: String): CommonFactory {
        LOGGER.info { "Init common factory" }
        cassandra.createConfig(keyspace = keyspace, internal = false).writeToFile(tmp.resolve(CRADLE_CONFIDENTIAL_FILE_NAME))
        rabbit.createConfig(internal = false).writeToFile(tmp.resolve(RABBIT_MQ_FILE_NAME))
        MessageRouterConfiguration(
            queues = mapOf(
                "estore" to QueueConfiguration(
                    routingKey = "",
                    queue = "estore",
                    exchange = RABBIT_MQ_EXCHANGE,
                    attributes = listOf(QueueAttribute.PUBLISH.value, QueueAttribute.EVENT.value)
                ),
                "codec-out" to QueueConfiguration(
                    routingKey = "codec-out",
                    queue = "",
                    exchange = RABBIT_MQ_EXCHANGE,
                    attributes = listOf(QueueAttribute.PUBLISH.value, "group"),
                ),
                "codec-in" to QueueConfiguration(
                    routingKey = "",
                    queue = "codec-in",
                    exchange = RABBIT_MQ_EXCHANGE,
                    attributes = listOf(QueueAttribute.SUBSCRIBE.value, "group"),
                )
            )
        ).writeToFile(tmp.resolve(ROUTER_MQ_FILE_NAME))
        return CommonFactory.createFromArguments("-c", tmp.toString())
    }

    private fun initCodecMock(factory: CommonFactory) {
        factory.messageRouterMessageGroupBatch.apply {
            subscribeAll { _, message ->
                val newBuilder = MessageGroupBatch.newBuilder()
                message.groupsList.forEach { group ->
                    val groupsBuilder = newBuilder.addGroupsBuilder()
                    group.messagesList.forEach {
                        when {
                            it.hasMessage() -> groupsBuilder.addMessages(it)
                            it.hasRawMessage() -> groupsBuilder += it.rawMessage.toParsed()
                        }
                    }
                }
                sendAll(newBuilder.build())
            }
        }
    }

    @BeforeEach
    fun setup() {
        if (::client.isInitialized) {
            client.channel.apply {
                if (this is ManagedChannel) {
                    shutdown()
                    awaitTermination(5_000, TimeUnit.MILLISECONDS)
                }
            }
        }
        client = createGrpcStub()
    }

    @AfterAll
    fun afterAll() {
        LOGGER.info { "Closing resources" }
        runCatching { if (::localCommonFactory.isInitialized) localCommonFactory.close() }.onFailure { LOGGER.error(it) { "cannot close common factory" } }
        stopAll(provider, cassandra, rabbit)
        LOGGER.info { "Resources closed" }
    }

    @Test
    fun `extracts messages by group`() {
        val bookName = "test"
        val group = "test-group"
        val count: Long = 100
        val aliases = 5
        val (start, end) = storeMessages(bookName, aliases, group, count)

        val results = withDeadlineAfter(Duration.ofMillis(3000)) {
            searchMessageGroups(
                MessageGroupsSearchRequest
                    .newBuilder()
                    .setBookId(BookId.newBuilder().setName(bookName))
                    .addMessageGroup(MessageGroupsSearchRequest.Group.newBuilder().setName(group))
                    .setStartTimestamp(start.toTimestamp())
                    .setEndTimestamp(end.plusNanos(1).toTimestamp())
                    .setSort(BoolValue.of(true))
                    .build()
            )
        }.asSequence().filter { it.hasMessage() }.toList()

        validateResponse(results, aliases * count, aliases, count)
    }

    @Test
    fun `extracts messages by alias`() {
        val bookName = "messages"
        val group = "test-alias-0"
        val count: Long = 1000
        val aliases = 1
        val (start, end) = storeMessages(
            bookName,
            aliases,
            group,
            count,
            aliasPattern = "test-alias-%d",
            messagesPerBatch = 100,
            directionSupplier = { Direction.FIRST })

        val results = withDeadlineAfter(Duration.ofMillis(3000)) {
            searchMessages(
                MessageSearchRequest.newBuilder()
                    .setStartTimestamp(start.toTimestamp())
                    .setEndTimestamp(end.plusNanos(1).toTimestamp())
                    .setBookId(BookId.newBuilder().setName(bookName))
                    .addStream(MessageStream.newBuilder().setName(group).setDirection(com.exactpro.th2.common.grpc.Direction.FIRST))
                    .build()
            )
        }.asSequence().filter { it.hasMessage() }.toList()

        validateResponse(results, count, aliases, count)
    }

    private fun storeMessages(
        bookName: String,
        aliasesCount: Int,
        group: String,
        messagesPerAlias: Long,
        startTimestamp: Instant = Instant.now().minus(1, ChronoUnit.HOURS),
        aliasPattern: String = "test-%d",
        messagesPerBatch: Long = messagesPerAlias,
        directionSupplier: (Int) -> Direction = { if (it % 2 == 0) Direction.FIRST else Direction.SECOND },
    ): Pair<Instant, Instant> {
        var lastTimestamp: Instant = startTimestamp
        localCommonFactory.cradleManager.storage.apply {
            val book = addBook(BookToAdd(bookName))
            val bookWithPage = addPage(book.id, "test_page", startTimestamp, "test page")

            LOGGER.info { "Storing messages" }
            repeat(aliasesCount) {
                var generated = 0L
                val alias = aliasPattern.format(it)
                do {
                    storeGroupedMessageBatch(entitiesFactory.groupedMessageBatch(group).apply {
                        val firstTimestamp = lastTimestamp
                        val toGenerate = min(messagesPerBatch, messagesPerAlias - generated)
                        createMessageToStore(
                            alias = alias,
                            generateMessageSequence(),
                            firstTimestamp,
                            count = toGenerate,
                            direction = directionSupplier(it),
                            increment = Duration.ofNanos(10),
                            maxTimestamp = Instant.MAX,
                            bookId = bookWithPage.id,
                        ).forEach(::addMessage)

                        generated += toGenerate

                        lastTimestamp = getLastTimestamp()
                    })
                } while (generated != messagesPerAlias)
                LOGGER.info { "Stored $generated message for alias $alias" }
            }
            LOGGER.info { "Messages stored" }
        }
        return Pair(startTimestamp, lastTimestamp)
    }

    private fun <T> withDeadlineAfter(
        duration: Duration,
        action: DataProviderGrpc.DataProviderBlockingStub.() -> T
    ): T {
        check(::client.isInitialized) { "gRPC client is not initialized" }
        return client.withDeadlineAfter(duration, action)
    }

    private inline fun <T> DataProviderGrpc.DataProviderBlockingStub.withDeadlineAfter(
        duration: Duration,
        crossinline action: DataProviderGrpc.DataProviderBlockingStub.() -> T
    ): T {
        return Context.current().withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS, executor).call { action() }
    }

    @OptIn(ExperimentalStdlibApi::class)
    private fun validateResponse(
        results: List<MessageSearchResponse>,
        expectedMessagesCount: Long,
        expectedAliases: Int,
        messagesPerAlias: Long,
    ) {
        Assertions.assertEquals(expectedMessagesCount, results.size.toLong(), "Unexpected number of messages received")
        val notParsedMessage = results.filter { it.message.messageItemList.isEmpty() }
        Assertions.assertTrue(notParsedMessage.isEmpty()) {
            "${notParsedMessage.size} message(s) do(es) not have parsed message: ${notParsedMessage.joinToString { it.toJson() }}"
        }
        val unordered: List<MessageSearchResponse> = buildList {
            fun MessageSearchResponse.timestamp(): Instant = message.timestamp.toInstant()
            var prevResp: MessageSearchResponse? = null
            for (it in results) {
                if (prevResp != null && prevResp.timestamp() > it.timestamp()) {
                    add(prevResp)
                }
                prevResp = it
            }
        }
        Assertions.assertTrue(unordered.isEmpty()) {
            "${unordered.size} message(s) was unordered: ${unordered.joinToString { it.toJson() }}"
        }
        val messagesByAlias = results.groupingBy { it.message.messageId.connectionId.sessionAlias }
            .eachCount()
        Assertions.assertEquals(expectedAliases, messagesByAlias.size) {
            "Unexpected number of aliases: ${messagesByAlias.keys}"
        }
        Assertions.assertAll(messagesByAlias.map { (alias, count) ->
            Executable {
                Assertions.assertEquals(messagesPerAlias, count.toLong()) {
                    "Unexpected number of messages for alias $alias"
                }
            }
        })
    }

    private fun createGrpcStub(): DataProviderGrpc.DataProviderBlockingStub {
        val host = provider.host
        val port = provider.getMappedPort(ProviderContainer.DEFAULT_OUT_PORT)
        return DataProviderGrpc.newBlockingStub(
            ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build()
        )
    }

    private fun RabbitMQContainer.initQueues(queuesToRoutingKey: Map<String, List<String>>) {
        withExchange(RABBIT_MQ_EXCHANGE, "direct", false, false, true, emptyMap())
        for ((queue, routingKeys) in queuesToRoutingKey) {
            withQueue(queue)
            routingKeys.forEach { routingKey ->
                withBinding(RABBIT_MQ_EXCHANGE, queue, emptyMap(), routingKey, "queue")
            }
        }
    }

    private fun Any.asTransferable(): Transferable = mapper.writeValueAsBytes(this).run(Transferable::of)
    private fun Any.writeToFile(path: Path): Unit = mapper.writeValue(path.toFile(), this)

    private fun startAll(vararg startable: Startable) {
        Startables.deepStart(startable.asList()).get()
    }

    private fun stopAll(vararg startables: Startable) {
        startables.forEach { container ->
            runCatching { container.stop() }
                .onFailure { LOGGER.error(it) { "cannot stop container: ${container::class.java}" } }
        }
    }

    private fun DefaultCassandraContainer.createConfig(keyspace: String, internal: Boolean = true) = CradleConfidentialConfiguration(
        dataCenter = "datacenter1",
        host = if (internal) "cassandra" else host,
        keyspace = keyspace,
        port = if (internal) CassandraContainer.CQL_PORT else getMappedPort(CassandraContainer.CQL_PORT),
        username = username,
        password = password,
    )

    private fun RabbitMQContainer.createConfig(internal: Boolean = true): RabbitMQConfiguration = RabbitMQConfiguration(
        host = if (internal) "rabbit" else host,
        vHost = "",
        port = if (internal) 5672 else amqpPort,
        username = adminUsername,
        password = adminPassword,
    )

    companion object {
        private const val RABBIT_MQ_FILE_NAME = "rabbitMQ.json"
        private const val ROUTER_MQ_FILE_NAME = "mq.json"
        private const val GRPC_FILE_NAME = "grpc.json"
        private const val ROUTER_GRPC_FILE_NAME = "grpc_router.json"
        private const val CRADLE_CONFIDENTIAL_FILE_NAME = "cradle.json"
        private const val PROMETHEUS_FILE_NAME = "prometheus.json"
        private const val CUSTOM_FILE_NAME = "custom.json"
        private const val BOX_FILE_NAME = "box.json"
        private const val CONNECTION_MANAGER_CONF_FILE_NAME = "mq_router.json"
        private const val CRADLE_NON_CONFIDENTIAL_FILE_NAME = "cradle_manager.json"
        private const val RABBIT_MQ_EXCHANGE = "th2"
    }
}

private fun RawMessage.toParsed(): Message {
    return Message.newBuilder()
        .setMetadata(
            MessageMetadata.newBuilder()
                .setId(metadata.id)
                .setMessageType("Test")
                .setProtocol("test")
        )
        .build()
}
