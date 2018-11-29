import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import org.slf4j.LoggerFactory
import java.util.NoSuchElementException
import java.util.concurrent.LinkedBlockingDeque

const val PROJECT_ID = ""

object Main {


    private val SUBSCRIPTION_ID = ""


    private val logger = LoggerFactory.getLogger(Main::class.java)
    private val messages =  LinkedBlockingDeque<PubsubMessage>()

    private class DefaultMessageReceiver: MessageReceiver {
        override fun receiveMessage(message: PubsubMessage?, consumer: AckReplyConsumer?) {
            consumer?.ack()
        }
    }

    /**
     * Entrypoint for the service
     */
    @JvmStatic
    fun main(args: Array<String>) {
        val subscriptionName =
            ProjectSubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID)
        readExistingMessages(subscriptionName)
    }

    /**
     * Read existing messages in queue
     */
    private fun readExistingMessages(subscriptionName: ProjectSubscriptionName) {
        var subscriber: Subscriber? = null
        try {
            logger.info("Subscriber listening for messages")
            subscriber = Subscriber.newBuilder(subscriptionName,
                DefaultMessageReceiver()).build()
            subscriber.startAsync().awaitRunning()
            while (true) {
                logger.info("Reading messages in queue")
                val currentMessage = messages.take()
                logger.info("Received message ${currentMessage.messageId}")
                processMessage(currentMessage)
            }
        } catch (e: NoSuchElementException) {
            logger.info("No more messages in queue")
        }
        catch (e: Exception) {
            logger.error("Error with subscriber", e)
        } finally {
            subscriber?.let {
                it.stopAsync()
            }
        }
    }

    /**
     * Submit dataflow job
     */
    private fun processMessage(message: PubsubMessage) {
        logger.info("Submitting message ${message.messageId} to dataflow")
    }
}