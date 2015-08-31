package org.collokia.vertx.dynamodb.streams

import io.vertx.core.json.JsonObject

class DynamoDBMessageBusShardConsumerVerticle : AbstractDynamoDBShardConsumerVerticle() {

    private fun getAddress() = config().getString("address")

    override fun processRecords(records: List<JsonObject>) {
        records.forEach { recordJson ->
            vertx.eventBus().send(getAddress(), recordJson)
        }
    }

}