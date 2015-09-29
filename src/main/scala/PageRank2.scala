import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.slf4j.LoggerFactory
import org.apache.spark.SparkConf

object PageRank2 {
  val log = LoggerFactory.getLogger("PageRank")

  def main(args: Array[String]): Unit = {
    log.info("args: {}", args)

    val startTime = System.nanoTime()

    val sparkConf = new SparkConf
    sparkConf.setAppName("PageRank")
    
    val master = System.getProperty("master")
    if (master != null) {
      // VM Arguments: -Dmaster=local
      sparkConf.setMaster(master)
    }
    
    val sc = new SparkContext(sparkConf)
    
    var endTime = System.nanoTime()    
    log.info("Spark context loaded: {}", (endTime - startTime) / 1000000000d)
    
    val graph = GraphLoader.edgeListFile(sc, Config.resourcesPath + "network")
    endTime = System.nanoTime()    
    log.info("Graph loaded: {}", (endTime - startTime) / 1000000000d)

    val ranks = graph.pageRank(0.0001).vertices
    endTime = System.nanoTime()    
    log.info("Page rank executed: {}", (endTime - startTime) / 1000000000d)

    // Join the ranks with the URLs
    val urls = sc.textFile(Config.resourcesPath + "index").map {
      line =>
        val fields = line.split("\t")
        (fields(1).toLong, fields(0))
    }

    val ranksByUrl = urls.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    endTime = System.nanoTime()    
    log.info("Joined ranks with the URLs: {}", (endTime - startTime) / 1000000000d)

    val orderedRanks = ranksByUrl.sortBy(_._2, false)
    endTime = System.nanoTime()    
    log.info("Sorted ranks: {}", (endTime - startTime) / 1000000000d)
    
    // Save the results in text file
    //println(ranksByUrl.collect().mkString("\n"))
    orderedRanks.saveAsTextFile(Config.home + "scala-output-" + DateUtils.format())
    endTime = System.nanoTime()    
    log.info("Saved to text file: {}", (endTime - startTime) / 1000000000d)
  }

}
