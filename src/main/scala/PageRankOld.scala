import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.Logging

object PageRankOld extends Serializable with Logging {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local", "PageRank")
    
    val graph = GraphLoader.edgeListFile(sc, Config.resourcesPath + "followers.txt")

    val ranks = graph.pageRank(0.0001).vertices

    // Join the ranks with the usernames
    val users = sc.textFile(Config.resourcesPath + "users.txt").map {
      line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1))
    }

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    // Print the result
    println(ranksByUsername.collect().mkString("\n"))

  }

}
