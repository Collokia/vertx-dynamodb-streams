package org.collokia.vertx.dynamodb.streams

import io.vertx.ext.unit.TestContext
import org.junit.Test

class SomeTest : BaseDynamoDBLocalTest() {

    @Test
    fun testConsume(context: TestContext) {
        context.assertTrue(true)
    }

}