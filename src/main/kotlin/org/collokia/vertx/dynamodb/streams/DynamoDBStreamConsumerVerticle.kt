package org.collokia.vertx.dynamodb.streams

import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import org.collokia.vertx.util.getFromSharedMemoryAsync
import org.collokia.vertx.util.onSuccess
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class DynamoDBStreamConsumerVerticle : DynamoDBStreamVerticle() {

    companion object {
        private val log = LoggerFactory.getLogger("DynamoDBStreamConsumerVerticle")
    }

    private var shardVerticlesDeploymentIds = CopyOnWriteArrayList<kotlin.String>()

    override fun startAfterClientInit(startFuture: Future<Void>) {
        vertxClient.describeStream(getStreamArn(), null, null, onSuccess<JsonObject> {
            val shardIds = it.getJsonArray("shards").map { (it as? JsonObject)?.getString("shardId") }.filterNotNull()
            val latch = CountDownLatch(shardIds.size())

            for (shardId in shardIds) {
                vertx.getFromSharedMemoryAsync(DynamoDBStreamVerticle.ShardIteratorMapName, getShardIteratorKey(shardId), onSuccess<String?> { shardIterator ->
                    val shardVerticleConfig = config().copy()
                        .put("shardId", shardId)
                        .put("shardIterator", shardIterator)

                    val shardConsumerVerticleName = config().getString("shardConsumerVerticleName")
                    if (shardConsumerVerticleName == null) {
                        startFuture.fail("No 'shardConsumerVerticleName' specified")
                    } else {
                        vertx.deployVerticle(
                            shardConsumerVerticleName,
                            DeploymentOptions().setConfig(shardVerticleConfig),
                            onSuccess<String> {
                                shardVerticlesDeploymentIds.add(it)
                                latch.countDown()
                            } onFail {
                                log.error("Can't start shard consumer verticle", it)
                            }
                        )
                    }
                } onFail {
                    log.error("Can't retrieve shard iterator from shared memory", it)
                })
            }

            if (latch.await(10, TimeUnit.SECONDS)) {
                log.info("Deployed ${ shardIds.size() } shard consumer verticles")
                startFuture.complete()
            } else {
                startFuture.fail("Can't initialize shard consumers")
            }
        } onFail {
            startFuture.fail(it)
        })
    }



}