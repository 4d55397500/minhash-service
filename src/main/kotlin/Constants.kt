import java.util.*

const val BQ_PROJECT = ""
const val BQ_DATASET = ""
const val BQ_MINHASHES_TABLE = "minhashes"
const val BQ_HASHMAP_TABLE = "hashmap"


const val LARGE_PRIME = 4294967311
val RNG = Random(System.currentTimeMillis())
val MAX_BYTE = Math.pow(2.0, 32.0).toInt()
