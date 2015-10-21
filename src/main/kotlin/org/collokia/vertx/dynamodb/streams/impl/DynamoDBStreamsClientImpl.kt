package org.collokia.vertx.dynamodb.streams.impl

import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsAsync
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsAsyncClient
import com.amazonaws.services.dynamodbv2.model.*
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import org.collokia.vertx.dynamodb.streams.DynamoDBStreamsClient
import org.collokia.vertx.dynamodb.streams.util.toByteArray
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.properties.Delegates

class DynamoDBStreamsClientImpl(val vertx: Vertx, val config: JsonObject) : DynamoDBStreamsClient {

    companion object {
        private val log = LoggerFactory.getLogger("vertx-dynamodb-streams")
    }

    var client: AmazonDynamoDBStreamsAsync by Delegates.notNull()

    private var initialized = AtomicBoolean(false)

    override fun describeStream(streamArn: String, limit: Int?, exclusiveStartShardId: String?, resultHandler: Handler<AsyncResult<JsonObject>>) {
        withClient { client ->
            client.describeStreamAsync(DescribeStreamRequest()
                .withStreamArn(streamArn)
                .withLimit(limit)
                .withExclusiveStartShardId(exclusiveStartShardId),
                resultHandler.withConverter {
                    it.streamDescription.let { stream ->
                        JsonObject()
                            .put("streamArn", stream.streamArn)
                            .put("streamLabel", stream.streamLabel)
                            .put("streamStatus", stream.streamStatus)
                            .put("streamViewType", stream.streamViewType)
                            .put("creationRequestDateTime", stream.creationRequestDateTime.time)
                            .put("lastEvaluatedShardId", stream.lastEvaluatedShardId)
                            .put("tableName", stream.tableName)
                            .put("keySchema", JsonArray(stream.keySchema.map { keySchemaElement ->
                                JsonObject()
                                    .put("attributeName", keySchemaElement.attributeName)
                                    .put("keyType", keySchemaElement.keyType)
                            }))
                            .put("shards", JsonArray(stream.shards.map { shard ->
                                JsonObject()
                                    .put("shardId", shard.shardId)
                                    .put("parentShardId", shard.parentShardId)
                                    .put("sequenceNumberRange", shard.sequenceNumberRange.let { range ->
                                        JsonObject()
                                            .put("startingSequenceNumber", range.startingSequenceNumber)
                                            .put("endingSequenceNumber", range.endingSequenceNumber)
                                    })
                            }))
                    }
                }
            )
        }
    }

    override fun getRecords(shardIterator: String, limit: Int?, resultHandler: Handler<AsyncResult<JsonObject>>) {
        withClient { client ->
            client.getRecordsAsync(GetRecordsRequest()
                .withShardIterator(shardIterator)
                .withLimit(limit),
                resultHandler.withConverter {
                    JsonObject()
                        .put("nextShardIterator", it.nextShardIterator)
                        .put("records", JsonArray(it.records?.map { record ->
                            JsonObject()
                                .put("eventID", record.eventID)
                                .put("eventName", record.eventName)
                                .put("eventVersion", record.eventVersion)
                                .put("eventSource", record.eventSource)
                                .put("awsRegion", record.awsRegion)
                                .put("record", record.dynamodb?.let { dynamoDb ->
                                    JsonObject()
                                        .put("keys", dynamoDb.keys?.toJson())
                                        .put("newImage", dynamoDb.newImage?.toJson())
                                        .put("oldImage", dynamoDb.oldImage?.toJson())
                                        .put("sequenceNumber", dynamoDb.sequenceNumber)
                                        .put("sizeBytes", dynamoDb.sizeBytes)
                                        .put("streamViewType", dynamoDb.streamViewType)
                                })
                        }))
                }
            )
        }
    }

    private fun Map<String, AttributeValue>.toJson(): JsonObject =
        JsonObject(this.mapValues { it.value.toJson() })

    private fun AttributeValue.toJson(): JsonObject = JsonObject()
        .put("stringData", this.s)
        .put("numberData", this.n)
        .put("binaryData", this.b?.toByteArray())
        .put("stringListData", this.ss?.toJsonArray())
        .put("numberListData", this.ns?.toJsonArray())
        .put("binaryListData", this.bs?.map { it.toByteArray() }?.let { JsonArray(it) })
        .put("map", this.m?.mapValues { it.value.toJson() }?.let { JsonObject(it) })
        .put("list", this.l?.map { it.toJson() }?.let { JsonArray(it) })
        .put("boolean", this.isBOOL)
        .put("isNull", this.isNULL)

    private fun List<String>.toJsonArray(): JsonArray? {
        if (isEmpty()) {
            return null
        }
        return JsonArray(this)
    }

    override fun getShardIterator(streamArn: String, shardId: String, shardIteratorType: String, sequenceNumber: String?, resultHandler: Handler<AsyncResult<String>>) {
        withClient { client ->
            client.getShardIteratorAsync(GetShardIteratorRequest()
                .withStreamArn(streamArn)
                .withShardId(shardId)
                .withShardIteratorType(shardIteratorType)
                .withSequenceNumber(sequenceNumber),
                resultHandler.withConverter { it.shardIterator }
            )
        }
    }

    override fun listStreams(tableName: String, limit: Int?, exclusiveStartStreamArn: String?, resultHandler: Handler<AsyncResult<List<JsonObject>>>) {
        withClient { client ->
            client.listStreamsAsync(ListStreamsRequest()
                .withTableName(tableName)
                .withLimit(limit)
                .withExclusiveStartStreamArn(exclusiveStartStreamArn),
                resultHandler.withConverter {
                    it.streams.map { stream ->
                        JsonObject()
                            .put("streamArn", stream.streamArn)
                            .put("tableName", stream.tableName)
                            .put("streamLabel", stream.streamLabel)
                    }
                }
            )
        }
    }

    override fun start(resultHandler: Handler<AsyncResult<Void>>) {
        log.info("Starting DynamoDB Streams client");

        vertx.executeBlocking(Handler { future ->
            try {
                val credentials: AWSCredentials = if (config.getString("accessKey") != null) {
                    BasicAWSCredentials(config.getString("accessKey"), config.getString("secretKey"))
                } else {
                    try {
                        ProfileCredentialsProvider().credentials
                    } catch (t: Throwable) {
                        throw AmazonClientException(
                            "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format."
                        )
                    }
                }

                client = AmazonDynamoDBStreamsAsyncClient(credentials)

                val region = config.getString("region")
                client.setRegion(Region.getRegion(Regions.fromName(region)))
                if (config.getString("host") != null && config.getInteger("port") != null) {
                    client.setEndpoint("http://${ config.getString("host") }:${ config.getInteger("port") }")
                }

                initialized.set(true)

                future.complete()
            } catch (t: Throwable) {
                future.fail(t)
            }
        }, true, resultHandler)
    }

    override fun stop(resultHandler: Handler<AsyncResult<Void>>) {
        resultHandler.handle(Future.succeededFuture()) // nothing
    }

    private fun withClient(handler: (AmazonDynamoDBStreamsAsync) -> Unit) {
        if (initialized.get()) {
            handler(client)
        } else {
            throw IllegalStateException("DynamoDB Streams client wasn't initialized")
        }
    }

    fun <DynamoDBRequest : AmazonWebServiceRequest> Handler<AsyncResult<Void?>>.toDynamoDBHandler(): AsyncHandler<DynamoDBRequest, Void?> = withConverter { it }

    fun <DynamoDBRequest : AmazonWebServiceRequest, DynamoDBResult, VertxResult> Handler<AsyncResult<VertxResult>>.withConverter(
            converter: (DynamoDBResult) -> VertxResult
    ): DynamoDBToVertxHandlerAdapter<DynamoDBRequest, DynamoDBResult, VertxResult> =
        DynamoDBToVertxHandlerAdapter(
            vertxHandler                = this,
            dynamoDBResultToVertxMapper = converter
        )

    class DynamoDBToVertxHandlerAdapter<DynamoDBRequest : AmazonWebServiceRequest, DynamoDBResult, VertxResult>(
        val vertxHandler: Handler<AsyncResult<VertxResult>>,
        val dynamoDBResultToVertxMapper: (DynamoDBResult) -> VertxResult
    ) : AsyncHandler<DynamoDBRequest, DynamoDBResult> {

        override fun onSuccess(request: DynamoDBRequest, result: DynamoDBResult) {
            try {
                val vertxResult = dynamoDBResultToVertxMapper(result)
                vertxHandler.handle(Future.succeededFuture(vertxResult))
            } catch (t: Throwable) {
                vertxHandler.handle(Future.failedFuture(t))
            }
        }

        override fun onError(exception: Exception) {
            vertxHandler.handle(Future.failedFuture(exception))
        }
    }
    
}