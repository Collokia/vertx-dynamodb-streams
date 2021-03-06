package org.collokia.vertx.dynamodb.streams;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.collokia.vertx.dynamodb.streams.impl.DynamoDBStreamsClientImpl;

import java.util.List;

@VertxGen
public interface DynamoDBStreamsClient {

    static DynamoDBStreamsClient create(Vertx vertx, JsonObject config) {
        return new DynamoDBStreamsClientImpl(vertx, config);
    }

    void describeStream(String streamArn, Integer limit, String exclusiveStartShardId, Handler<AsyncResult<JsonObject>> resultHandler);

    void getRecords(String shardIterator, Integer limit, Handler<AsyncResult<JsonObject>> resultHandler);

    void getShardIterator(String streamArn, String shardId, String shardIteratorType, String sequenceNumber, Handler<AsyncResult<String>> resultHandler);

    void listStreams(String tableName, Integer limit, String exclusiveStartStreamArn, Handler<AsyncResult<List<JsonObject>>> resultHandler);

    void start(Handler<AsyncResult<Void>> resultHandler);

    void stop(Handler<AsyncResult<Void>> resultHandler);


}
