
import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math.min
@SerialVersionUID(123L)
case class Object(Node:Long, aGrp:Long, bGrp:Long) extends Serializable{}
   object Graph{
      def main (args: Array[String]){
         val conf = new SparkConf().setAppName("Assignment5")
         val sc = new SparkContext(conf)

         // A graph is a dataset of vertices, where each vertex is a triple
         // (group,id,adj) where id is the vertex id, group is the group id
         // (initially equal to id), and adj is the list of outgoing neighbors
	 // read the graph from the file args(0)
         var graph: RDD[ ( Long, Long, List[Long] ) ]
            = sc.textFile(args(0)).map( line => { val a = line.split(",")     
                                          (a(0).toLong,a(0).toLong,a.drop(1).toList.map(_.toLong)) })
         var graph2 = graph.map(g => (g._1,g))
         graph2.foreach(println)      

         for ( i <- 1 to 5 ) {
         // For each vertex (group,id,adj) generate the candidate (id,group)
         // and for each x in adj generate the candidate (x,group).
         // Then for each vertex, its new group number is the minimum candidate
            graph = graph.flatMap(
            map => map match{ case (x, y, xy) => (x, y) :: xy.map(z => (z,y))})
           .reduceByKey((a, b) => (if (a >= b) b else a))
           .join(graph2).map(g => (g._2._2._2, g._2._1, g._2._2._3))

      }

      // reconstruct the graph using the new group numbers
      // print the group sizes
      val groups = graph.map(g => (g._2, 1))
      .reduceByKey((m, n) => (m + n))
      .sortBy(_._1)
      .collect()
      .foreach(println)       
      sc.stop()
    }
}
