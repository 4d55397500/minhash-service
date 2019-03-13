import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList
import org.apache.beam.sdk.values.TypeDescriptors
import org.slf4j.LoggerFactory
import java.util.*


const val BQ_DATASET = ""
const val BQ_TABLE = ""

const val LARGE_PRIME = 4294967311
val RNG = Random(System.currentTimeMillis())
val MAX_BYTE = Math.pow(2.0, 32.0).toInt()

data class HashFunctionParameters(val params: List<Pair<Int, Int>>)

fun generateHashFunctionParameters(n: Int): HashFunctionParameters {
    val params = (0 until n)
        .map { Pair(1 + RNG.nextInt(MAX_BYTE) - 1, 1 + RNG.nextInt(MAX_BYTE) - 1) }
    return HashFunctionParameters(params)
}

fun sourcesWithKeys(p: Pipeline, sources: List<Pair<String, String>>): PCollection<KV<String, String>> {
    return PCollectionList.of(sources.map { p
            .apply(FileIO.match().filepattern(it.second))
            .apply(FileIO.readMatches())
            .apply(
                MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                    .via { f -> KV.of(it.first, f.readFullyAsUTF8String()) }
            )
    }).apply(Flatten.pCollections())
}


// maps source strings to min hashes
class MinHashFn(
    private val n: Int, private val k: Int): DoFn<KV<String, String>, KV<String, Array<Int>>>() {

    lateinit var hashFunctionParameters: HashFunctionParameters

    @Setup
    fun setup() {
        hashFunctionParameters = generateHashFunctionParameters(n)
    }

    @ProcessElement
    fun processElement(c: ProcessContext) {
        val key = c.element().key
        val doc = c.element().value
        val shingles = computeShingles(doc, k)
        val minHashes = computeMinHashes(shingles, hashFunctionParameters)
        c.output(KV.of(key, minHashes))
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


internal fun computeMinHashes(s: Set<Int>, minHashParams: HashFunctionParameters): Array<Int> {
    return minHashParams.params.map { param ->
        val minHash = s.map { applyHashFunction(it, param.first, param.second) }
            .min()
            ?.toInt() ?: Int.MAX_VALUE
        minHash
    }.toTypedArray()
}

internal fun applyHashFunction(obj: Int, a: Int, b: Int) = (a * obj + b) % LARGE_PRIME


/**
 * Compute k-shingles, returned in compressed 4-byte representation
 */
internal fun computeShingles(doc: String, k: Int): Set<Int> {
    val tokens = doc.split(" ")
    val shingles = if (tokens.size < k) { setOf() } else (0..tokens.size - k).map {
        hashString(tokens.subList(it, it + k).joinToString(" ")) }.toSet()
    return shingles
}


/**
 * Hashes a string to a compressed 4-byte representation
 */
internal fun hashString(s: String): Int {
    return getModulo(s.hashCode(), Math.pow(2.0, 32.0).toInt())
}

// n % d where  d is a power of two
internal fun getModulo(n: Int, d: Int): Int {
    return n and (d-1)
}


