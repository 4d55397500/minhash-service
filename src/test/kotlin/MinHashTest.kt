import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.junit.Test


class MinHashTest {

    companion object {
        val sampleDocs = mapOf(
            "doc1" to "this is a sample document",
            "doc2" to "this is another sample document",
            "doc3" to "short document"
        )
        val sampleSourcePaths = mapOf(
            "doc1" to "src/main/resources/foo.txt",
            "doc2" to "src/main/resources/bar.txt"
        )
    }

    @Test
    fun `minhash locality`() {
        val minhashes = (1..10).map {
            val doc = "This is a sample tex$it a lot of text and some of who knows what foo${it}bar"
            docToMinHash(doc, 3, mockHashFunctionParameters(20))
        }
        var ctEqual = 0
        minhashes.flatMap { mh -> minhashes.map { Pair(it, mh)}.filter { !it.first.equals(it.second) } }
        .map {
            val a = it.first.toSet()
            val b = it.second.toSet()
            a.intersect(b).size * 1.0 / a.union(b).size
        }.forEach {
            if (it == 1.0) {
                ctEqual += 1
            }
        }
        assert (ctEqual > 50)

    }

    @Test
    fun `verify minhashfn preserves locality`() {
        val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java)
        options.stableUniqueNames = PipelineOptions.CheckEnabled.OFF
        val p = TestPipeline.create(options)
        val input = p.apply(Create.of(listOf(KV.of("key1", "This is a text"),
            KV.of("key2", "This is a text"))))
        val minHashFn = MinHashFn(3, 2, mock = true)
        val output = input.apply(ParDo.of(minHashFn))
        val correctMinHashes = arrayOf(-1233248911, -1233248911, -1233248911)
        // checks operation is idempotent
        PAssert.that(output).containsInAnyOrder(
            KV.of("key1", correctMinHashes),
            KV.of("key2", correctMinHashes))

        val input2 = p.apply(Create.of(listOf(
            KV.of("key1", "This is a somewhat long text about something or other"),
            KV.of("key2", "This is a somewhat kind of long text about something or another"))))
        val minHashFn2 = MinHashFn(10, 2, mock = true)
        val output2 = input2.apply(ParDo.of(minHashFn2))

        // checks locality
        val minhashes2 = arrayOf(-1768377226, -1768377226, -1768377226, -1768377226, -1768377226, -1768377226, -1768377226, -1768377226, -1768377226, -1768377226)
        PAssert.that(output2).containsInAnyOrder(
            KV.of("key1", minhashes2),
            KV.of("key2", minhashes2))
        p.run()
    }

    @Test
    fun `end-to-end dataflow pipeline test behaves correctly`() {
        val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java)
        options.stableUniqueNames = PipelineOptions.CheckEnabled.OFF
        val p = TestPipeline.create(options)
        val pcol1 = sourcesWithSha1Key(p, sampleSourcePaths.toList())
        val minHashFn = MinHashFn(3, 2, mock = true)
        val pcol2 = pcol1.apply(ParDo.of(minHashFn))
        PAssert.that(pcol1).containsInAnyOrder(
            KV.of("7f2887e2208387289018604b951bb637b7fcc4048899d50e4d3c579561119a48", "this is a foo text"),
            KV.of("6fdb7c443b01a2aadbb7bb8067c1efdbeb4367abd2fe24e05cbaf37dca10e597", "this is a bar text"))
        PAssert.that(pcol2).containsInAnyOrder(
            KV.of("7f2887e2208387289018604b951bb637b7fcc4048899d50e4d3c579561119a48", arrayOf(-728103049, -728103049, -728103049)),
            KV.of("6fdb7c443b01a2aadbb7bb8067c1efdbeb4367abd2fe24e05cbaf37dca10e597", arrayOf(-993485849, -993485849, -993485849))
        )
        p.run()
    }

    @Test
    fun `BigQuery hash table pcollection formed without error`() {
        val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java)
        options.stableUniqueNames = PipelineOptions.CheckEnabled.OFF
        val p = TestPipeline.create(options)
        val testInputs = listOf(KV.of("key1", arrayOf(1, 2)),
            KV.of("key2", arrayOf(3, 4)))
        val pcol = p.apply(Create.of(testInputs))
        buildBigQueryHashTable(pcol)
        p.run()
    }

    @Test
    fun `test buildBigQueryHashTableFn`() {
        val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java)
        options.stableUniqueNames = PipelineOptions.CheckEnabled.OFF
        val p = TestPipeline.create(options)
        val testInputs = listOf(KV.of("key1", arrayOf("foo", "bar")),
            KV.of("key2", arrayOf("foo", "bar")))
        val pcol = p.apply(Create.of(testInputs))
        pcol.apply(ParDo.of(BigQueryHashMapFn()))
        p.run()
    }

    @Test
    fun `test combine by key works as expected`() {
        val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java)
        options.stableUniqueNames = PipelineOptions.CheckEnabled.OFF
        val p = TestPipeline.create(options)
        val testInputs = listOf(KV.of("key1", "foo"),
            KV.of("key1", "bar"))
        val pcol = p.apply(Create.of(testInputs))
        val output = pcol.apply(Combine.perKey<String, String, Array<String>>(CombineDocHashes()))
        PAssert.that(output)
            .containsInAnyOrder(
                KV.of("key1", arrayOf("bar", "foo"))
            )
        p.run()
    }


    @Test
    fun `the minhash operation produces proper behavior`() {
        val s = setOf(12312, 231321, 412421)
        val minHashes = computeMinHashes(s, mockHashFunctionParameters(2))
        assert (minHashes.toList() == listOf(9037534, 9037534)) {
            "incorrect minhashes"
        }
    }

    @Test
    fun `minhash FileReaderFn operates correctly`() {
        val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java)
        options.stableUniqueNames = PipelineOptions.CheckEnabled.OFF
        val p = TestPipeline.create(options)
        val pcol1 = sourcesWithOriginalKey(p, sampleSourcePaths.toList())
        PAssert.that(pcol1).containsInAnyOrder(
            KV.of("doc1", "this is a foo text\n"),
            KV.of("doc2", "this is a bar text\n"))
        p.run()
    }

    @Test
    fun `the minhash DoFn function operates correctly`() {
        val fnTester =
            DoFnTester.of(MinHashFn(n = 3, k = 2, mock = true))
        val testInput = KV.of("doc1", sampleDocs["doc1"]!!)
        val testOutputs = fnTester.processBundle(testInput)
        assert (testOutputs.size == 1 && testOutputs.first().value.size == 3) {
            "incorrect number of minhashes"
        }
        assert (testOutputs.first().value.toList() == listOf(-1592000835, -1592000835, -1592000835)) {
            "incorrect minhash values"
        }
    }

    @Test
    fun `k-shingles are extracted correctly`() {
        assert (computeShingles(sampleDocs["doc1"]!!, 3).size == 3) {
            "incorrect number of shingles"
        }
        assert(computeShingles(sampleDocs["doc1"]!!, 23).isEmpty()) {
            "expected empty set for shingle number greater than token count"
        }
        computeShingles(sampleDocs["doc1"]!!, 3).forEach {
            assert (it < Math.pow(2.0, 32.0)) { "expected 4-byte representation for k-shingles"}
        }
    }

}