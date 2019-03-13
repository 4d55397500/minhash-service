import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.DoFnTester
import org.apache.beam.sdk.transforms.ParDo
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
            "doc1" to "gcs://foo/foo.txt",
            "doc2" to "gcs://bar/bar.txt"
        )
    }

    @Test
    fun `end-to-end dataflow pipeline test behaves correctly`() {
        val p = TestPipeline.create()
        val pcol1 = sourcesWithKeys(p, sampleSourcePaths.toList())
        val pcol2 = pcol1.apply(ParDo.of(MinHashFn(3, 2)))
        // PAssert.that() ....
    }


    @Test
    fun `the minhash operation produces proper behavior`() {
        val s = setOf(12312, 231321, 412421)
        val minHashes = computeMinHashes(s, generateHashFunctionParameters(2))
    }

    @Test
    fun `the minhash DoFn function operates correctly`() {
        val fnTester =
            DoFnTester.of(MinHashFn(n = 3, k = 2))
        val testInput = KV.of("doc1", sampleDocs["doc1"]!!)
        val testOutputs = fnTester.processBundle(testInput)
        assert (testOutputs.size == 1 && testOutputs.first().value.size == 3) {
            "incorrect number of minhashes"
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