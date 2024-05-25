import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    // Read the graph from the file
    var graph: RDD[(Long, Long, List[Long])] = sc.textFile(args(0))
      .map { line =>
        val parts = line.split(",").map(_.toLong)
        (parts(0), parts(0), parts.tail.toList)
      }

    for (_ <- 1 to 5) {
      // Generate group candidates
      val candidates: RDD[(Long, Long)] = graph.flatMap {
        case (group, id, adj) => (id, group) :: adj.map((_, group))
      }

      // Find minimum new group for each vertex
      val groups: RDD[(Long, Long)] = candidates
        .reduceByKey(math.min)

      // Join the groups with the graph to update group id and maintain the adjacency list
      graph = graph.map {
        case (_, id, adj) => (id, (id, adj))
      }.join(groups).map {
        case (id, ((_, adj), newGroup)) => (newGroup, id, adj)
      }
    }

    // Calculate and print the size of each group
    // Calculate and print the size of each group
   graph.map {
   case (group, _, _) => (group, 1)
   }.reduceByKey(_ + _)
   .sortByKey()
   .collect()
   .foreach {
      case (group, size) => println(s"$group\t$size")
   }


    sc.stop()
  }
}
