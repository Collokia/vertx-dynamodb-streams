package org.collokia.vertx.dynamodb.streams

import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import org.collokia.vertx.util.putToSharedMemoryAsync
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.properties.Delegates

abstract class AbstractDynamoDBShardConsumerVerticle : DynamoDBStreamVerticle() {

    companion object {
        private val log = LoggerFactory.getLogger("DynamoDBShardConsumerVerticle")
        private val DefaultPollingInterval: Long = 500
    }

    private var timerId: Long by Delegates.notNull()
    private var shardIterator: String by Delegates.notNull()
    private var shouldStop = AtomicBoolean(false)


    override fun startAfterClientInit(startFuture: Future<Void>) {
        val initialShardIterator = config().getString("shardIterator")

        fun scheduleGetRecords() {
            timerId = vertx.setPeriodic(config().getLong("pollingInterval") ?: DefaultPollingInterval) {
                routeRecords()
            }
        }

        if (initialShardIterator != null) {
            shardIterator = initialShardIterator
            log.info("Starting shard consumer with iterator = $shardIterator")
            scheduleGetRecords()
            startFuture.complete()
        } else {
            log.info("Starting shard consumer without initial iterator") // TODO: ????????
            vertxClient.getShardIterator(getStreamArn(), getShardId(), "TRIM_HORIZON", null, Handler {
                if (it.succeeded()) {
                    shardIterator = it.result()
                    scheduleGetRecords()
                    startFuture.complete()
                } else {
                    startFuture.fail(it.cause())
                }
            })
        }
    }

    protected abstract fun processRecords(records: List<JsonObject>)

    private fun routeRecords() {
        if (shouldStop.get()) {
            return
        }

        vertxClient.getRecords(shardIterator, null, Handler {
            if (it.succeeded()) {
                val records = it.result().getJsonArray("records").map { it as? JsonObject }.filterNotNull()
                processRecords(records)

                val nextShardIterator = it.result().getString("nextShardIterator")
                if (nextShardIterator != null) {
                    shardIterator = nextShardIterator
                    vertx.putToSharedMemoryAsync(DynamoDBStreamVerticle.ShardIteratorMapName, getShardIteratorKey(getShardId()), nextShardIterator, Handler {
                        if (!it.succeeded()) {
                            log.error("Unable to store next shard iterator to shared memory", it.cause())
                        }
                    })
                } else {
                    shouldStop.set(true)
                    log.info("No more records would be available from the iterator for shard ${ getShardId() }")
                }
            } else {
                log.error("Unable to get records from shard ${ getShardId() } of stream ${ getStreamArn() }", it.cause())
            }
        })
    }

    override fun stopBeforeClientDispose() {
        vertx.cancelTimer(timerId)
    }

    private fun getShardId() = config().getString("shardId")

}