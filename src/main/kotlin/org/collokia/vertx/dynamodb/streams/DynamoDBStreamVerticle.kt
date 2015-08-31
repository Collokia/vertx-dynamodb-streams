package org.collokia.vertx.dynamodb.streams

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import org.collokia.vertx.dynamodb.streams.impl.DynamoDBStreamsClientImpl
import kotlin.properties.Delegates

abstract class DynamoDBStreamVerticle : AbstractVerticle() {

    companion object {
        val ShardIteratorMapName = "dynamodb-sharditerator"
    }

    protected var vertxClient: DynamoDBStreamsClient by Delegates.notNull()

    final override fun start(startFuture: Future<Void>) {
        startBeforeClientInit()

        vertxClient = DynamoDBStreamsClientImpl(vertx, config())
        vertxClient.start {
            if (it.succeeded()) {
                startAfterClientInit(startFuture)
            } else {
                startFuture.fail(it.cause())
            }
        }
    }

    final override fun stop(stopFuture: Future<Void>) {
        stopBeforeClientDispose()

        vertxClient.stop {
            if (it.succeeded()) {
                stopAfterClientDispose(stopFuture)
            } else {
                stopFuture.fail(it.cause())
            }
        }
    }

    open protected fun startAfterClientInit(startFuture: Future<Void>) { startFuture.complete() }

    open protected fun stopAfterClientDispose(stopFuture: Future<Void>) { stopFuture.complete() }

    open protected fun startBeforeClientInit() {}

    open protected fun stopBeforeClientDispose() {}

    protected fun getStreamArn(): String = config().getString("streamArn")

    protected fun getShardIteratorKey(shardId: String): String = "${ getStreamArn() }-$shardId"

}