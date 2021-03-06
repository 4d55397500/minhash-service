
import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.google.common.hash.Hashing
import org.apache.beam.repackaged.beam_runners_core_java.com.google.common.collect.ImmutableList
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList



fun runPipelineMockDocs() {

    val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java)
    options.project = BQ_PROJECT
    options.stagingLocation = "gs://dataflowtemp/staging"
    options.runner = DataflowRunner::class.java
    options.stableUniqueNames = PipelineOptions.CheckEnabled.OFF

    val p = Pipeline.create(options)

    val sourcesCollection = p.apply(Create.of((1..1000).map {
        KV.of("doc$it", "This is a sample tex$it a lot of text and some of who knows what foo${it}bar")
    }))

    val minHashesCollection =
        sourcesCollection.apply(ParDo.of(MinHashFn(20, 3)))
    val bqHashTableRows =
        buildBigQueryHashTable(minHashesCollection)
    val bqMinHashesTableRows =
        buildBigQueryMinHashTable(minHashesCollection)

    val hashTableSchema = TableSchema()
        .setFields(ImmutableList.of(
            TableFieldSchema()
                .setName("partialMinHash")
                .setType("STRING")
                .setMode("REQUIRED"),
            TableFieldSchema()
                .setName("docHashes")
                .setType("STRING")
                .setMode("REPEATED")
        ))

    bqHashTableRows.apply(BigQueryIO.writeTableRows()
        .to("$BQ_PROJECT:$BQ_DATASET.$BQ_HASHMAP_TABLE")
        .withSchema(hashTableSchema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

    val minHashSchema = TableSchema()
        .setFields(ImmutableList.of(
            TableFieldSchema()
                .setName("key")
                .setType("STRING")
                .setMode("REQUIRED"),
            TableFieldSchema()
                .setName("minHashes")
                .setType(("INT64"))
                .setMode("REPEATED")
        ))
    bqMinHashesTableRows.apply(BigQueryIO.writeTableRows()
        .to("$BQ_PROJECT:$BQ_DATASET.$BQ_MINHASHES_TABLE")
        .withSchema(minHashSchema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

    p.run()
}

fun runPipeline(sources: List<Pair<String, String>>) {

    val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java)
    options.project = BQ_PROJECT
    options.stagingLocation = "gs://dataflowtemp/staging"
    options.runner = DataflowRunner::class.java
    options.stableUniqueNames = PipelineOptions.CheckEnabled.OFF

    val p = Pipeline.create(options)

    val sourcesCollection = sourcesWithOriginalKey(p, sources)

    val minHashesCollection =
        sourcesCollection.apply(ParDo.of(MinHashFn(10, 2)))
    val bqHashTableRows =
        buildBigQueryHashTable(minHashesCollection)
    val bqMinHashesTableRows =
        buildBigQueryMinHashTable(minHashesCollection)

    val hashTableSchema = TableSchema()
        .setFields(ImmutableList.of(
            TableFieldSchema()
                .setName("partialMinHash")
                .setType("STRING")
                .setMode("REQUIRED"),
            TableFieldSchema()
                .setName("docHashes")
                .setType("STRING")
                .setMode("REPEATED")
        ))

    bqHashTableRows.apply(BigQueryIO.writeTableRows()
        .to("$BQ_PROJECT:$BQ_DATASET.$BQ_HASHMAP_TABLE")
        .withSchema(hashTableSchema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

    val minHashSchema = TableSchema()
        .setFields(ImmutableList.of(
         TableFieldSchema()
             .setName("key")
             .setType("STRING")
             .setMode("REQUIRED"),
         TableFieldSchema()
             .setName("minHashes")
             .setType(("INT64"))
             .setMode("REPEATED")
        ))
    bqMinHashesTableRows.apply(BigQueryIO.writeTableRows()
        .to("$BQ_PROJECT:$BQ_DATASET.$BQ_MINHASHES_TABLE")
        .withSchema(minHashSchema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE))

    p.run()
}



data class HashFunctionParameters(val params: List<Pair<Int, Int>>)


fun generateHashFunctionParameters(n: Int): HashFunctionParameters {
    val params = (0 until n)
        .map { Pair(1 + RNG.nextInt(MAX_BYTE) - 1, 1 + RNG.nextInt(MAX_BYTE) - 1) }
    return HashFunctionParameters(params)
}

fun mockHashFunctionParameters(n: Int): HashFunctionParameters {
    val params = (0 until n)
        .map { Pair(1 + n * 367 - 1, 1 + n * 263 - 1) }
    return HashFunctionParameters(params)
}

@Deprecated("using provided document key instead")
fun sourcesWithSha1Key(p: Pipeline, sources: List<Pair<String, String>>): PCollection<KV<String, String>> {
    return PCollectionList.of(sources.mapIndexed { i,  it ->
        p.apply("readSources_$i", TextIO.read().from(it.second))
         .apply("addSha256Key_$i", ParDo.of(Sha256HashFn()))
    }).apply("flatten", Flatten.pCollections())
}

fun sourcesWithOriginalKey(p: Pipeline, sources: List<Pair<String, String>>): PCollection<KV<String, String>> {
    return PCollectionList.of(sources.mapIndexed { i, it ->
        val documentKey = it.first
        val documentPath = it.second
        p.apply("readFiles_$i", FileIO.match().filepattern(documentPath))
            .apply("readMatches_$i", FileIO.readMatches())
            .apply("readFiles_$i", ParDo.of(FileReaderFn(documentKey)))
    }).apply("flatten", Flatten.pCollections())
}

class FileReaderFn(private val documentKey: String): DoFn<FileIO.ReadableFile, KV<String, String>>() {
    @ProcessElement
    fun processElement(c: ProcessContext) {
        val readableFile = c.element()
        c.output(KV.of(documentKey, readableFile.readFullyAsUTF8String()))
    }
}


class Sha256HashFn: DoFn<String, KV<String, String>>() {
    @ProcessElement
    fun processElement(c: ProcessContext) {
        c.output(KV.of(Hashing.sha256().hashString(c.element(), Charsets.UTF_8).toString(), c.element()))
    }
}

fun buildBigQueryHashTable(minHashPCollection: PCollection<KV<String, Array<Int>>>): PCollection<TableRow> {
    val pcol = minHashPCollection
        .apply(ParDo.of(PartialHashesFn(3)))
        .apply(Combine.perKey<String, String, Array<String>>(CombineDocHashes()))
    return pcol.apply(ParDo.of(BigQueryHashMapFn()))
}


fun buildBigQueryMinHashTable(minHashPCollection: PCollection<KV<String, Array<Int>>>): PCollection<TableRow> {
    return minHashPCollection
        .apply(ParDo.of(BigQueryMinHashTableFn()))
}



// maps <doc hash, minhashes> -> multiple <partial min hash, doc hash>
// each partial hash a dash-concatened string eg. 122,133,134 -> '122-133-134'
class PartialHashesFn(private val m: Int): DoFn<KV<String, Array<Int>>, KV<String, String>>() {
    @ProcessElement
    fun processElement(c: ProcessContext) {
        val docHash = c.element().key
        val minhashes = c.element().value
        partialHashes(m, minhashes).forEach {
            c.output(KV.of(it.joinToString("-"), docHash))
        }
    }
}

class CombineDocHashes: Combine.CombineFn<String, MutableList<String>, Array<String>>() {
    override fun createAccumulator(): MutableList<String> {
        return mutableListOf()
    }

    override fun addInput(accumulator: MutableList<String>?, input: String?): MutableList<String> {
       if (accumulator != null) {
           accumulator.add(input!!)
           return accumulator
       } else {
           return mutableListOf()
       }
    }

    override fun mergeAccumulators(accumulators: MutableIterable<MutableList<String>>?): MutableList<String> {
        if (accumulators != null) {
            return accumulators.toList().flatMap { it }.toMutableList()
        } else {
            return mutableListOf()
        }
    }

    override fun extractOutput(accumulator: MutableList<String>?): Array<String> {
        if (accumulator != null) {
            return accumulator.sorted().toTypedArray()
        } else {
            return arrayOf()
        }
    }

}

// maps source strings to min hashes
class MinHashFn(
    private val n: Int, private val k: Int,
    private val mock: Boolean = false): DoFn<KV<String, String>, KV<String, Array<Int>>>() {
    lateinit var hashFunctionParameters: HashFunctionParameters

    @Setup
    fun setup() {
        hashFunctionParameters = if (mock) mockHashFunctionParameters(n) else generateHashFunctionParameters(n)
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

// BigQuery table with schema document sha1 hash, minhashes (each minhash its own column)
class BigQueryMinHashTableFn: DoFn<KV<String, Array<Int>>, TableRow>() {
    @ProcessElement
    fun processElement(c: ProcessContext) {
        c.output(TableRow()
            .set("key", c.element().key)
            .set("minHashes", c.element().value.map { it.toLong() }) // change to each min hash its own column
        )
    }
}


// BigQuery table with schema partial min-hash, array of document sha1 hashes matching that partial min-hash
class BigQueryHashMapFn: DoFn<KV<String, Array<String>>, TableRow>() {
    @ProcessElement
    fun processElement(c: ProcessContext) {
        val partialMinHash = c.element().key
        val docHashes = c.element().value.toList()
        c.output(TableRow()
            .set("partialMinHash", partialMinHash)
            .set("docHashes", docHashes))
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

internal fun docToMinHash(doc: String, k: Int, minHashParams: HashFunctionParameters): Array<Int> {
    return computeMinHashes(computeShingles(doc, k), minHashParams)
}

fun partialHashes(m: Int, minhashes: Array<Int>): List<Array<Int>> {
    return (0 .. minhashes.size - m).map { minhashes.sliceArray(it until it + m) }
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

fun main(args: Array<String>) {

    val localSources = listOf(
        Pair("doc1", "gs://sampledocs/doc1.txt"),
        Pair("doc2", "gs://sampledocs/doc2.txt"),
        Pair("doc3", "gs://sampledocs/doc3.txt")
    )

//    val sources = listOf(
//        Pair("alice", "gs://sampledocs/alice.txt"),
//        Pair("pride", "gs://sampledocs/pride.txt")
//    )
    //runPipeline(localSources)
    runPipelineMockDocs()
}