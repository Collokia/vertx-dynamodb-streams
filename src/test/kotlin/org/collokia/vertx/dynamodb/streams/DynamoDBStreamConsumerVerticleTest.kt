package org.collokia.vertx.dynamodb.streams

import com.amazonaws.services.dynamodbv2.model.*
import io.vertx.core.DeploymentOptions
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(VertxUnitRunner::class)
class DynamoDBStreamConsumerVerticleTest : BaseDynamoDBLocalTest() {

    companion object {
        val TableName = "MyTable"

        val Address = "dynamodb-streams-MyStream"

        val Config = JsonObject()
            .put("region", "us-west-2")
            .put("address", Address)
            .put("host", "localhost")
            .put("port", BaseDynamoDBLocalTest.Port)
            .put("shardConsumerVerticleName", "org.collokia.vertx.dynamodb.streams.DynamoDBMessageBusShardConsumerVerticle")
    }

    @Test
    fun testConsume(context: TestContext) {
        // Let's init DB client and create a table with a stream enabled
        val client = getDBClient()

        client.createTableAsync(CreateTableRequest()
            .withTableName(TableName)
            .withKeySchema(listOf(KeySchemaElement("name", KeyType.HASH)))
            .withAttributeDefinitions(listOf(AttributeDefinition("name", ScalarAttributeType.S)))
            .withStreamSpecification(StreamSpecification()
                .withStreamEnabled(true)
                .withStreamViewType(StreamViewType.NEW_IMAGE)
            )
            .withProvisionedThroughput(ProvisionedThroughput(10, 10)),
            context.onSuccess {
                // The table is created
                // Now let's create & init a Vertx DynamoDB Streams client
                val dynamoDBStreamsClient = DynamoDBStreamsClient.create(BaseDynamoDBLocalTest.vertx, Config)
                dynamoDBStreamsClient.start(context.asyncAssertSuccess() {
                    dynamoDBStreamsClient.listStreams(TableName, null, null, context.asyncAssertSuccess() {
                        val stream = it.firstOrNull { it.getString("tableName") == TableName }
                        context.assertNotNull(stream)

                        val streamArn = stream?.getString("streamArn")
                        context.assertNotNull(streamArn)

                        // Now we deploy the consumer verticles
                        val consumerConfig = Config.copy().put("streamArn", streamArn)
                        BaseDynamoDBLocalTest.vertx.deployVerticle("org.collokia.vertx.dynamodb.streams.DynamoDBStreamConsumerVerticle", DeploymentOptions().setConfig(consumerConfig), context.asyncAssertSuccess() {
                            val messageReceiveAsync = context.async()

                            // Stream/shard consumer verticles are deployed, let's start listening to the address configured
                            BaseDynamoDBLocalTest.vertx.eventBus().consumer(Address, Handler { message: Message<JsonObject> ->
                                context.assertEquals("someName", message.body().getJsonObject("record")?.getJsonObject("keys")?.getJsonObject("name")?.getString("stringData"))
                                messageReceiveAsync.complete()
                            })

                            // Now let's put something in a table and see if we get anything from the stream
                            client.putItemAsync(PutItemRequest(TableName, mapOf("name" to AttributeValue("someName"))), context.onSuccess {
                                // Items are put, now we wait
                            })
                        })
                    })
                })
            }
        )
    }

}