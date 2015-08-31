package org.collokia.vertx.dynamodb.streams

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import io.vertx.ext.unit.TestContext
import org.junit.Test

class DynamoDBStreamConsumerVerticleTest : BaseDynamoDBLocalTest() {

    companion object {
        val TableName = "MyTable"
    }

    @Test
    fun testConsume(context: TestContext) {
        // Let's init DB client and create a table
        val client = getDBClient()
        client.createTableAsync(CreateTableRequest().withTableName(TableName), context.onSuccess {
            // TODO: WIP
            println(it.getTableDescription())
        })
    }

}