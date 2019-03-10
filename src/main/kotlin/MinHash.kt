import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.slf4j.LoggerFactory
import java.util.*


const val BQ_DATASET = ""
const val BQ_TABLE = ""

const val P = 17027399.toLong()
const val M = 16890581.toLong()

/**
 * Minhashing with Dataflow
 * @param key A string identifier for the document(s)
 * @param key The Google Cloud Storage path of the corresponding document(s)
 */
class MinHash(
    private val key: String,
    private val documentPath: String
) {

    companion object {
        private val logger = LoggerFactory.getLogger(MinHash::class.java)
    }

    /**
     * Submits the job
     */
    fun submit() {
        logger.info("Creating dataflow job")
        val pipeline = Pipeline.create(PipelineOptionsFactory.create())
        val sourceLines = pipeline.apply(TextIO.read().from(documentPath))
        val minHashes = ParDo.of(MinHashFn(nHashes = 10, maxNGrams = 3))
            .expand(sourceLines)
        val tableRows = ParDo.of(BigQueryFn(PROJECT_ID, BQ_DATASET, BQ_TABLE)).expand(minHashes)
        BigQueryIO.writeTableRows().to("$PROJECT_ID:$BQ_DATASET.$BQ_TABLE")
        pipeline.run()
        logger.info("Submitted dataflow job")
    }
}

/**
 * A dataflow dofn mapping documents to min hash values
 * @param number of hashes to use in the min-hash representation of the corresponding document
 * @param maxNGrams maximum n-grams size. Will extract all n-grams with
 * n up to and including this value
 */
class MinHashFn(
    private val nHashes: Int,
    private val maxNGrams: Int): DoFn<String, KV<String, Array<Long>>>() {

    lateinit var minHashParams: List<Pair<Int, Int>>

    @Setup
    fun setup() {
        val rng = Random(System.currentTimeMillis())
        minHashParams = (0 until nHashes)
            .map {  Pair(1+ rng.nextInt(P.toInt() - 1), rng.nextInt(P.toInt())) }
    }

    @ProcessElement
    fun processElement(c: ProcessContext) {
        val tokens = c.element().split(" ")
        //TODO("finish minhashing")

    }
}

/**
 * Writes min-hashes to BigQuery
 */
class BigQueryFn(
    private val projectId: String,
    private val datasetName: String,
    private val tableName: String
): DoFn<KV<String, Array<Long>>, TableRow>() {

    @ProcessElement
    fun processElement(c: ProcessContext) {
        c.output(TableRow()
            .set("key", c.element().key)
            .set("minHashes", c.element().value)
        )
    }

}

/**
 * min-hashes a set of objects
 */
internal fun <T> minHash(s: Set<T>): Array<Long> {
    //TODO("implement minHash")
    return arrayOf()
}

