import com.google.cloud.bigquery.*
import java.util.*


object LocalSearch {

    /**
     * @param n minhash length
     * @param m partial minhash length
     * @param nNeighbors number of neighbors to look for
     */
    fun constructQuery(docKeys: List<String>, n: Int, m: Int, nNeighbors: Int): String {
        return """
            WITH eph AS (
                SELECT
                    key,
                    ${extractAllPartialMinHashes(n, m)} partialHashes
                FROM
                    `$BQ_PROJECT.$BQ_DATASET.$BQ_MINHASHES_TABLE`
                WHERE
                    key IN (${docKeys.map { "'$it'" }.joinToString(", ")})
            )
            SELECT
                eph.key,
                APPROX_TOP_COUNT(flattenedDocHashes, ${nNeighbors + 1})
            FROM
               `$BQ_PROJECT.$BQ_DATASET.$BQ_HASHMAP_TABLE`, eph, UNNEST(docHashes) flattenedDocHashes
            WHERE
                partialMinHash IN UNNEST(eph.partialHashes)
            GROUP BY
                eph.key
        """.trimIndent()
    }

    private fun extractAllPartialMinHashes(n: Int, m: Int): String {
        return (0 .. n - m).map { extractPartialMinHash(it, it + m) }
            .joinToString(",\n", prefix = "[", postfix = "]")
    }

    private fun extractPartialMinHash(i: Int, j: Int): String {
        return "ARRAY_TO_STRING(ARRAY(SELECT CAST(num AS STRING) FROM UNNEST(minHashes) num WITH OFFSET pos WHERE pos >= $i AND pos < $j), '-')"
    }

    fun findNeighbors(bigQuery: BigQuery, docKeys: List<String>, n: Int, m: Int, nNeighbors: Int): Map<String, List<DocRank>> {
        val query = constructQuery(docKeys, n, m, nNeighbors)
        val queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build()
        val jobId = UUID.randomUUID().toString()
        val queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(JobId.of(jobId)).build())
            .waitFor()
        val result = queryJob.getQueryResults()
        return result.iterateAll().map {
            val key = it[0].stringValue
            val l = it[1].repeatedValue.map {
                val doc = it.recordValue[0].stringValue
                val count = it.recordValue[1].longValue
                DocRank(doc, count)
            }.filter { it.docKey != key }.sortedByDescending { it.score }
            Pair(key, l)
        }.toMap()
    }

    data class DocRank(val docKey: String, val score: Long)
}

fun main(args: Array<String>) {
    //println(LocalSearch.constructQuery(listOf("doc207", "doc339"), 20, 3, 10))
    val bigquery = BigQueryOptions.getDefaultInstance().service
    LocalSearch.findNeighbors(bigquery, listOf("doc207", "doc339"), 20, 3, 10)
}