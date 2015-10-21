package org.collokia.vertx.dynamodb.streams

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
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
import java.lang.ProcessBuilder.Redirect
import kotlin.properties.Delegates

// -DDynamoDB.Local.Path=$MODULE_DIR$/lib/dynamodb_local_2015-07-16_1.0.zip
@RunWith(VertxUnitRunner::class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
abstract class BaseDynamoDBLocalTest {

    companion object {
        val DynamoDBLocalZipPath: String? = System.getProperty("DynamoDB.Local.Path")
        val Port = 8123
        val vertx: Vertx = Vertx.vertx()
        var dynamoDBLocalProcess: Process by Delegates.notNull()

        @BeforeClass
        @JvmStatic
        fun before(context: TestContext) {
            context.assertNotNull(DynamoDBLocalZipPath)
            context.assertTrue(File(DynamoDBLocalZipPath).exists())

            val localDynamoDBPath = File(System.getProperty("user.home"), ".localDynamoDb-vertx-dynamodb-streams")
            if (!localDynamoDBPath.exists()) {
                localDynamoDBPath.mkdir()
                ZipFile(DynamoDBLocalZipPath).extractAll(localDynamoDBPath.path)
            }

            val localDynamoDBJar = File(localDynamoDBPath, "DynamoDBLocal.jar")
            context.assertTrue(localDynamoDBJar.exists())

            dynamoDBLocalProcess = ProcessBuilder()
                .command(listOf("java", "-jar", localDynamoDBJar.path, "-inMemory", "-port", Port.toString()))
                .directory(localDynamoDBPath)
                .redirectOutput(Redirect.INHERIT)
                .redirectError(Redirect.INHERIT)
                .start()

            Thread.sleep(2000)

            context.assertTrue(dynamoDBLocalProcess.isAlive)
        }

        @AfterClass
        @JvmStatic
        fun after(context: TestContext) {
            dynamoDBLocalProcess.destroy()
        }
    }

    fun <DynamoDBRequest : AmazonWebServiceRequest, DynamoDBResult> TestContext.onSuccess(success: (DynamoDBResult) -> Unit): AsyncHandler<DynamoDBRequest, DynamoDBResult> {
        return this.asyncAssertSuccess<DynamoDBResult>().onSuccess(this) { success(it) }
    }

    fun <DynamoDBRequest : AmazonWebServiceRequest, DynamoDBResult> Handler<AsyncResult<DynamoDBResult>>.onSuccess(context: TestContext, success: (DynamoDBResult) -> Unit): AsyncHandler<DynamoDBRequest, DynamoDBResult> {
        val vertxAsyncResultHandler = this
        val async = context.async()

        return object: AsyncHandler<DynamoDBRequest, DynamoDBResult> {
            override fun onSuccess(request: DynamoDBRequest, result: DynamoDBResult) {
                vertxAsyncResultHandler.handle(Future.succeededFuture(result))

                try {
                    success(result)
                } finally {
                    async.complete()
                }
            }

            override fun onError(exception: Exception) {
                vertxAsyncResultHandler.handle(Future.failedFuture(exception))
                async.complete()
            }
        }
    }

    protected fun getDBClient(): AmazonDynamoDBAsync {
        val credentials: AWSCredentials = ProfileCredentialsProvider().credentials
        val client = AmazonDynamoDBAsyncClient(credentials)
        client.setEndpoint("http://localhost:$Port")
        return client
    }

}