import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.slf4j.LoggerFactory

object PageRank2 {
  val log = LoggerFactory.getLogger("PageRank")

  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()

    val sc = new SparkContext("local", "PageRank")
    var endTime = System.nanoTime()    
    log.info("Spark context loaded: {}", (endTime - startTime) / 1000000000d)
    
    val graph = GraphLoader.edgeListFile(sc, Config.resourcesPath + "network")
    endTime = System.nanoTime()    
    log.info("Graph loaded: {}", (endTime - startTime) / 1000000000d)

    val ranks = graph.pageRank(0.0001).vertices
    endTime = System.nanoTime()    
    log.info("Page rank executed: {}", (endTime - startTime) / 1000000000d)

    // Join the ranks with the URLs
    val index = sc.textFile(Config.resourcesPath + "index").map {
      line =>
        val fields = line.split("\t")
        (fields(1).toLong, fields(0))
    }

    val ranksByUrl = index.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    endTime = System.nanoTime()    
    log.info("Joined ranks with the URLs: {}", (endTime - startTime) / 1000000000d)

    // Print the result
    //println(ranksByUrl.collect().mkString("\n"))
    ranksByUrl.saveAsTextFile(Config.home + "scala-output-" + DateUtils.format())
  }

}
