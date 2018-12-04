package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

object Problem2 {
  def main(args: Array[String]) {
    /* INITIALIZATION */
    val inputFile = args(0)
    val outputFolder = args(1)
    val t = args(2).toLong
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    
    /* FUNCTIONS */
    val initialMsg = Int.MaxValue
    def vprog(vID: VertexId, attr: (Int, Int), msg: Int): (Int, Int) = {
      // attr: (newState, oldState); 0: not visited, 1: visited
      if (msg == initialMsg)
        if (vID == t)
          (1, 0)  // mark node t as visited
        else
          attr    // ignore all other nodes so that they remain inactive
      else
        (msg max attr._1, attr._1)
    }
    def sendMsg(triplet: EdgeTriplet[(Int, Int), String]): Iterator[(VertexId, Int)] = {
      val srcAttr = triplet.srcAttr
      if (srcAttr._1 == srcAttr._2)
        Iterator.empty  // don't send anything if source node has been visited
      else
        Iterator((triplet.dstId, srcAttr._1))
    }
    def mergeMsg(msg1: Int, msg2: Int): Int = msg1 max msg2
    
    /* CORE */
    val input = sc.textFile(inputFile)
    // Extract "FromNodeId" and convert it into Long
    val from = input.map(_.split(" ")(1).toLong)
    // Extract "ToNodeId" and convert it into Long
    val to = input.map(_.split(" ")(2).toLong)
    
    // Merge arrays of FromNodeId and ToNodeId, extract array of distinct NodeId, and map it as VertexRDD
    val vertices = (from ++ to).distinct.map(v => (v, (0, 0)))
    // Join arrays of FromNodeId and ToNodeId, and map it as EdgeRDD
    val edges = (from zip to).map{case (v1, v2) => Edge(v1, v2, "")}
    // Build the graph
    val graph = Graph(vertices, edges)
    // Obtain subgraph containing node t
    val subgraph = graph.pregel(initialMsg, Int.MaxValue, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)
    // Count the number of nodes connected to t
    val connectedNodes = subgraph.vertices.map{case (vID, (attr1, attr2)) => attr1}.sum.toInt - 1
    
    sc.parallelize(Array(connectedNodes)).saveAsTextFile(outputFolder)
  }
}