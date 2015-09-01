# Amazon DynamoDB Streams Client for Vert.x
This Vert.x client allows Amazon DynamoDB Streams access in two ways:

* As a @VertxGen service bridge to Amazon DynamoDB Streams API Async Client methods
* As Amazon DynamoDB Streams stream/shard consuming verticles

## Service usage

Client can be configured with AWS credentials, otherwise a default ~/.aws/credentials credentials file will be used:

```
JsonObject config = new JsonObject()
    .put("accessKey", "someAccessKey")
    .put("secretKey", "someSecretKey")
```

The client is initialized asynchronously:

```
DynamoDBStreamsClient client = DynamoDBStreamsClient.create(vertx, config);
client.start(result -> {
    if (result.succeeded()) {
        System.out.println("Client is initialized");
    }
});
```

Once the client is initialized, it can be used to access the Amazon Kinesis API in async manner:

```
client.listStreams("MyTable", null, null, ar -> {
    if (ar.succeeded()) {
        List<JsonObject> streams = ar.result();
        ...
    }
});
```        
        
## Stream/shard consuming verticle usage

DynamoDB Streams stream/shard consumer verticles can be set up to process DynamoDB Streams events. A stream consumer verticle is deployed, which in turn deploys a shard consuming verticle for each stream's shard discovered.

The stream consuming verticle is deployed with a config containing AWS credentials (see above), stream ARN, and a name of shard consuming verticle to deploy for each shard:

```
JsonObject config = new JsonObject()
    .put("accessKey", "someAccessKey")
    .put("secretKey", "someSecretKey")
    .put("streamArn", "arn:aws:dynamodb:us-west-2:100012356789:table/TestTable/stream/2015-01-01T12:00:00.000")
    .put("shardConsumerVerticleName", "com.example.MyShardConsumingVerticle")

vertx.deployVerticle("org.collokia.vertx.dynamodb.streams.DynamoDBStreamConsumerVerticle", new DeploymentOptions().setConfig(config));    
```

When the stream verticle is deployed, it deploys a shard verticle for each stream's shard. The verticle to be deployed this way must be a subclass of `org.collokia.vertx.dynamodb.streams.AbstractDynamoDBShardConsumerVerticle`:

```
public class MyShardConsumingVerticle extends AbstractDynamoDBShardConsumerVerticle {
    @Override
    protected void processRecords(List<? extends JsonObject> records) {
        for (JsonObject record : records) {
            System.out.println(record);
        }
    }
}
```

It's deployed with a copy of a configuration passed to stream verticle plus shard metadata, so you can use the stream verticle configuration to pass the configuration data to the shard verticles.

### Shard iterators

In order to avoid consuming the same record twice, shard iterator is stored in Vertx's cluster memory (or local memory in case of no cluster present) async map named `dynamodb-sharditerator` with keys following the pattern `STREAM_NAME-SHARD_ID`.
