//Name : Prem Atul Jethwa
//UTA ID : 1001861810

//References - 
//https://spark.apache.org/docs/3.1.2/graphx-programming-guide.html#pregel-api
//https://spark.apache.org/docs/3.1.2/api/scala/org/apache/spark/graphx/Graph.html
//https://spark.apache.org/docs/3.1.2/api/scala/org/apache/spark/graphx/GraphOps.html
//https://spark.apache.org/docs/3.1.2/api/scala/org/apache/spark/graphx/EdgeTriplet.html

import org.apache.spark.graphx.{Graph => Graph, VertexId, Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object ConnectedComponents {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Connected Components")
    val sc = new SparkContext(conf)

    // graph edges have attribute values 0
    val edges: RDD[Edge[Long]]=sc.textFile(args(0)).map(line => { val (vertex, adjacent) = line.split(",").splitAt(1)
      (vertex(0).toLong,adjacent.toList.map(_.toLong))}).flatMap(e => e._2.map(v => (e._1,v))).map(node => Edge(node._1,node._2,node._1))
    
    // the GraphX graph
    val graph: Graph[Long, Long]=Graph.fromEdges(edges, "defValue").mapVertices((vid, _) => vid)
    
    // derive connected components using pregel
    val comps = graph.pregel(Long.MaxValue,5)(
    (id, old_Val, new_Val) => math.min(old_Val, new_Val),
    triplet => {
      if (triplet.attr < triplet.dstAttr)
      {
        Iterator((triplet.dstId,triplet.attr))
      }
      else if (triplet.srcAttr < triplet.attr)
      {
        Iterator((triplet.dstId,triplet.srcAttr))
      }
      else
      {
        Iterator.empty
      }
    },
    (x,y) => math.min(x,y)
    )
   // print the group sizes (sorted by group #)
    val result = comps.vertices.map(g => (g._2,1)).reduceByKey(_ + _).sortByKey().map(x => x._1.toString() + " " + x._2.toString())
    result.collect().foreach(println)
    
  }
}
