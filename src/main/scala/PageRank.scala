import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel

object PageRank extends Serializable with Logging {

  def main(args: Array[String]): Unit = {
    logInfo("args: " + args.mkString(" "))

    val msg = "Usage: PageRank <graphFile> <resultFile>" 
    require(args.length == 2, msg)  
    
    val graphFile = args(0)
//    val indexFile = args(1)
    val resultFile = args(1)
    
    val startTime = System.nanoTime()

    val sparkConf = new SparkConf
    sparkConf.setAppName("PageRank")

    val master = System.getProperty("master")
    if (master != null) {
      // VM Arguments: -Dmaster=local
      sparkConf.setMaster(master)
    }
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    val sc = new SparkContext(sparkConf)
    if (master != null && master.equals("local")) {
      // if master is different from local, use fs.defaultFS property from core-site.xml
      sc.hadoopConfiguration.set("fs.defaultFS", "file:///")
    }
    
    var endTime = System.nanoTime()    
    logInfo("Spark context loaded: " + ((endTime - startTime) / 1000000000d))
    
    val graph = GraphLoader.edgeListFile(sc, graphFile, false, sc.defaultParallelism, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
    endTime = System.nanoTime()    
    logInfo("Graph loaded: " + ((endTime - startTime) / 1000000000d))

    val ranks = graph.pageRank(0.0001).vertices
    endTime = System.nanoTime()    
    logInfo("Page rank executed: " + ((endTime - startTime) / 1000000000d))
    
    // Save the results in text file
    //println(ranksByUrl.collect().mkString("\n"))
    ranks.saveAsTextFile(resultFile)
    endTime = System.nanoTime()    
    logInfo("Saved to text file: " + ((endTime - startTime) / 1000000000d))
    
/*
    val ranks = graph.pageRank(0.0001).vertices
    endTime = System.nanoTime()    
    logInfo("Page rank executed: " + ((endTime - startTime) / 1000000000d))

    // Join the ranks with the URLs
    val urls = sc.textFile(indexFile).map {
      line =>
        val fields = line.split("\t")
        (fields(1).toLong, fields(0))
    }

    val ranksByUrl = urls.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    endTime = System.nanoTime()    
    logInfo("Joined ranks with the URLs: " + ((endTime - startTime) / 1000000000d))

    val orderedRanks = ranksByUrl.sortBy(_._2, false)
    endTime = System.nanoTime()    
    logInfo("Sorted ranks: " + ((endTime - startTime) / 1000000000d))
    
    // Save the results in text file
    //println(ranksByUrl.collect().mkString("\n"))
    orderedRanks.saveAsTextFile(Config.home + "scala-output-" + DateUtils.format())
    endTime = System.nanoTime()    
    logInfo("Saved to text file: " + ((endTime - startTime) / 1000000000d))
    * 
    */
  }

}
