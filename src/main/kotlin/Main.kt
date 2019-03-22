import com.google.gson.JsonParser
import io.javalin.Javalin

object Main {

    @JvmStatic
    fun main(args: Array<String>) {
        val app = Javalin.create().start(8080)
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
    }
}