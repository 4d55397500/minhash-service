import org.junit.Test


class MinHashTest {

    companion object {
        val sampleDocs = mapOf(
            "doc1" to "this is a sample document",
            "doc2" to "this is another sample document",
            "doc3" to "short document"
        )
    }

    @Test
    fun `the minhash operation produces proper behavior`() {
        val s1 = setOf(12312, 231321, 412421)
    }

    @Test
    fun `the minhash dataflow function operates correctly`() {

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