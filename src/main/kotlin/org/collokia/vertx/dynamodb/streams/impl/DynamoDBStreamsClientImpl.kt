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
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest
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
        private val log = LoggerFactory.getLogger(javaClass)
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
                    it.getStreamDescription().let { stream ->
                        JsonObject()
                            .put("streamArn", stream.getStreamArn())
                            .put("streamLabel", stream.getStreamLabel())
                            .put("streamStatus", stream.getStreamStatus())
                            .put("streamViewType", stream.getStreamViewType())
                            .put("creationRequestDateTime", stream.getCreationRequestDateTime().getTime())
                            .put("lastEvaluatedShardId", stream.getLastEvaluatedShardId())
                            .put("tableName", stream.getTableName())
                            .put("keySchema", JsonArray(stream.getKeySchema().map { keySchemaElement ->
                                JsonObject()
                                    .put("attributeName", keySchemaElement.getAttributeName())
                                    .put("keyType", keySchemaElement.getKeyType())
                            }))
                            .put("shards", JsonArray(stream.getShards().map { shard ->
                                JsonObject()
                                    .put("shardId", shard.getShardId())
                                    .put("parentShardId", shard.getParentShardId())
                                    .put("sequenceNumberRange", shard.getSequenceNumberRange().let { range ->
                                        JsonObject()
                                            .put("startingSequenceNumber", range.getStartingSequenceNumber())
                                            .put("endingSequenceNumber", range.getEndingSequenceNumber())
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
                        .put("nextShardIterator", it.getNextShardIterator())
                        .put("records", JsonArray(it.getRecords().map { record ->
                            JsonObject()
                                .put("eventID", record.getEventID())
                                .put("eventName", record.getEventName())
                                .put("eventVersion", record.getEventVersion())
                                .put("eventSource", record.getEventSource())
                                .put("awsRegion", record.getAwsRegion())
                                .put("record", record.getDynamodb().let { dynamoDb ->
                                    JsonObject()
                                        .put("keys", dynamoDb.getKeys())
                                        .put("newImage", dynamoDb.getNewImage())
                                        .put("oldImage", dynamoDb.getOldImage())
                                        .put("sequenceNumber", dynamoDb.getSequenceNumber())
                                        .put("sizeBytes", dynamoDb.getSizeBytes())
                                        .put("streamViewType", dynamoDb.getStreamViewType())
                                })
                        }))
                }
            )
        }
    }

    private fun Map<String, AttributeValue>.toJson(): JsonObject = JsonObject(
        this.mapValues { it.value.toJson() }
    )

    private fun AttributeValue.toJson(): JsonObject = JsonObject()
        .put("stringData", this.getS())
        .put("numberData", this.getN())
        .put("binaryData", this.getB()?.toByteArray())
        .put("stringListData", JsonArray(this.getSS()))
        .put("numberListData", JsonArray(this.getNS()))
        .put("binaryListData", JsonArray(this.getBS().map { it.toByteArray() }))
        .put("map", JsonObject(this.getM().mapValues { it.value.toJson() }))
        .put("list", JsonArray(this.getL().map { it.toJson() }))
        .put("boolean", this.isBOOL())
        .put("isNull", this.isNULL())

    override fun start(resultHandler: Handler<AsyncResult<Void>>) {
        log.info("Starting DynamoDB Streams client");

        vertx.executeBlocking(Handler { future ->
            try {
                val credentials: AWSCredentials = if (config.getString("accessKey") != null) {
                    BasicAWSCredentials(config.getString("accessKey"), config.getString("secretKey"))
                } else {
                    try {
                        ProfileCredentialsProvider().getCredentials()
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
            DynamoDBResultToVertxMapper  = converter
        )

    class DynamoDBToVertxHandlerAdapter<DynamoDBRequest : AmazonWebServiceRequest, DynamoDBResult, VertxResult>(
        val vertxHandler: Handler<AsyncResult<VertxResult>>,
        val DynamoDBResultToVertxMapper: (DynamoDBResult) -> VertxResult
    ) : AsyncHandler<DynamoDBRequest, DynamoDBResult> {

        override fun onSuccess(request: DynamoDBRequest, result: DynamoDBResult) {
            vertxHandler.handle(Future.succeededFuture(DynamoDBResultToVertxMapper(result)))
        }

        override fun onError(exception: Exception) {
            vertxHandler.handle(Future.failedFuture(exception))
        }
    }
    
}