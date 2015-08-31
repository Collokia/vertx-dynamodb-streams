package org.collokia.vertx.dynamodb.streams

import io.vertx.core.Vertx
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import net.lingala.zip4j.core.ZipFile
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.FixMethodOrder
import org.junit.runner.RunWith
import org.junit.runners.MethodSorters
import java.io.File
import kotlin.platform.platformStatic
import kotlin.properties.Delegates
import java.lang.ProcessBuilder.Redirect

// -DDynamoDB.Local.Path=$MODULE_DIR$/lib/dynamodb_local_2015-07-16_1.0.zip
@RunWith(VertxUnitRunner::class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
abstract class BaseDynamoDBLocalTest {

    companion object {
        val DynamoDBLocalZipPath: String? = System.getProperty("DynamoDB.Local.Path")

        val vertx: Vertx = Vertx.vertx()

        var dynamoDBLocalProcess: Process by Delegates.notNull()

        @BeforeClass
        @platformStatic
        fun before(context: TestContext) {
            context.assertNotNull(DynamoDBLocalZipPath)
            context.assertTrue(File(DynamoDBLocalZipPath).exists())

            val localDynamoDBPath = File(System.getProperty("user.home"), ".localDynamoDb-vertx-dynamodb-streams")
            if (!localDynamoDBPath.exists()) {
                localDynamoDBPath.mkdir()
                ZipFile(DynamoDBLocalZipPath).extractAll(localDynamoDBPath.getPath())
            }

            val localDynamoDBJar = File(localDynamoDBPath, "DynamoDBLocal.jar")
            context.assertTrue(localDynamoDBJar.exists())

            dynamoDBLocalProcess = ProcessBuilder()
                .command(listOf("java", "-jar", localDynamoDBJar.getPath(), "-inMemory"))
                .directory(localDynamoDBPath)
                .redirectOutput(Redirect.INHERIT)
                .redirectError(Redirect.INHERIT)
                .start()

            Thread.sleep(2000)

            context.assertTrue(dynamoDBLocalProcess.isAlive())
        }

        @AfterClass
        @platformStatic
        fun after(context: TestContext) {
            dynamoDBLocalProcess.destroy()
        }
    }

}