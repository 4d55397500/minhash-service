import com.google.cloud.bigquery.BigQueryOptions
import com.google.gson.*
import io.javalin.Javalin

object Main {

    @JvmStatic
    fun main(args: Array<String>) {
        val app = Javalin.create().start(8080)
        val bigquery = BigQueryOptions.getDefaultInstance().service
        app.get("/_health") { ctx -> ctx.status(200) }
        app.post("/dataflow") { ctx ->
            val body = ctx.body()
            val sources = JsonParser().parse(body).asJsonArray.map {
                val key = it.asJsonObject.get("key").asString
                val documentPath = it.asJsonObject.get("documentPath").asString
                Pair(key, documentPath)
            }.toList()
            runPipeline(sources)
        }
        app.post("/localsearch") { ctx ->
            val body = ctx.body()
            val docKeys = JsonParser().parse(body).asJsonArray.map { it.asString }.toList()
            val neighbors = LocalSearch.findNeighbors(bigquery, docKeys,20, 3, 10)
            val js = GsonBuilder().setPrettyPrinting().create().toJsonTree(neighbors)
            ctx.result(js.toString())
        }
    }
}