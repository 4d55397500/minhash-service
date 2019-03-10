import com.google.cloud.ServiceOptions
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.Subscriber
import com.google.gson.GsonBuilder
import com.google.gson.JsonParser
import com.google.protobuf.ByteString
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.ProjectTopicName
import com.google.pubsub.v1.PubsubMessage
import io.undertow.Handlers
import io.undertow.Undertow
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.server.handlers.PathHandler
import io.undertow.server.handlers.RedirectHandler
import io.undertow.server.handlers.ResponseCodeHandler
import io.undertow.server.handlers.encoding.ContentEncodingRepository
import io.undertow.server.handlers.encoding.EncodingHandler
import io.undertow.server.handlers.encoding.GzipEncodingProvider
import io.undertow.server.handlers.resource.ClassPathResourceManager
import io.undertow.server.handlers.resource.PathResourceManager
import io.undertow.server.handlers.resource.ResourceHandler
import io.undertow.util.Headers
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import java.util.*


const val PROJECT_ID = ""

val gson = GsonBuilder().setLenient().disableHtmlEscaping().setPrettyPrinting().create()

/**
 * Design: submit to pubsub messages each containing one to many
 * pairs <key, gcs document path> referencing documents to be minhashed
 * and stored.
 * The messages are sent to Dataflow (streaming job?) which performs the minhashing
 * and writes pairs <key, minhashes> to BigQuery.
 */
object Main {


    private val SUBSCRIPTION_ID = ""
    private val TOPIC = ""

    private val logger = LoggerFactory.getLogger(Main::class.java)

    private val publisher = Publisher.newBuilder(
        ProjectTopicName.of(ServiceOptions.getDefaultProjectId(), TOPIC))
        .build()


    /**
     * Entrypoint for the service
     */
    @JvmStatic
    fun main(args: Array<String>) {

        var handler: HttpHandler = ResponseCodeHandler.HANDLE_404
        handler = ResourceHandler(ClassPathResourceManager(Main::class.java.classLoader, "ui"), handler)
        handler = ResourceHandler(PathResourceManager(Paths.get("src/main/ui/build"), 0, true, true), handler)
        handler = PathHandler(handler)
            .addPrefixPath("/api/v0", Handlers.routing()
                .add("POST", "/pubsub/publish", prepareHandler(this::publish))
                .add("POST", "/pubsub/push", prepareHandler(this::pubsubPush))
                .setFallbackHandler(ResponseCodeHandler.HANDLE_404))
                .addExactPath("/_ah/health", ResponseCodeHandler.HANDLE_200)
                .addExactPath("/", RedirectHandler("/index.html"))

        handler = EncodingHandler(
            ContentEncodingRepository()
                .addEncodingHandler("gzip", GzipEncodingProvider(), 50))
            .setNext(handler)

        val port = 8080
        println("Listening on port $port ...")
        Undertow.builder()
            .addHttpListener(port, "0.0.0.0")
            .setHandler(handler)
            .build()
            .start()
    }

    private fun prepareHandler(handler: (exchange: HttpServerExchange) -> Unit): (exchange: HttpServerExchange) -> Unit = { exchange ->
        try {
            exchange.responseHeaders.add(Headers.CONTENT_TYPE, "application/prepareHandler; charset=UTF-8")
            handler(exchange)
        } catch (ex: Exception) {
            ex.printStackTrace()
            exchange.statusCode = 500
            exchange.responseSender.send(gson.toJson(mapOf("message" to ex.message)))
        }
    }

    private fun publish(exchange: HttpServerExchange) {
        val inputStream = exchange.inputStream
        val bytes = inputStream.readBytes()
        val pubsubMessage = PubsubMessage
            .newBuilder()
            .setData(ByteString.copyFrom(bytes))
            .build()
        publisher.publish(pubsubMessage)
        logger.info("Published message")
    }

    private fun pubsubPush(exchange: HttpServerExchange) {
        val requestBody = String(Base64.getDecoder().decode(exchange.inputStream.readBytes()))
        val json = JsonParser().parse(requestBody)
        val key = json.asJsonObject.get("key").asString
        val documentPath = json.asJsonObject.get("documentPath").asString
        logger.info("Submitting message to dataflow")
        MinHash(key, documentPath).submit()
    }


}