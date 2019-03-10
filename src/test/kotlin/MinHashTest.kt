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

    }

    @Test
    fun `the minhash dataflow function operates correctly`() {

    }

    @Test
    fun `ngrams are extracted properly`() {
        val gramExtractionA = computeNgrams(sampleDocs["doc1"]!!, 3)
        val gramExtractionB = computeNgrams(sampleDocs["doc3"]!!, 200)
        assert (gramExtractionA.size == 12) {
            "Incorrect number of grams for 3-gram extraction" }
        assert(gramExtractionA.containsAll(listOf("this", "sample document"))) {
            "missing grams in gramExtractionA"
        }
        assert(gramExtractionB.containsAll(listOf("short", "document", "short document"))) {
            "missing grams in gramExtractionB"
        }
        assert (gramExtractionB.size == 3) {
            "Failed on 200-grams extraction on a short document"
        }
    }
}