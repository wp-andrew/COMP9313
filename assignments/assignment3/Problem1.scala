package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.reflect.classTag

object Problem1 {
  def main(args: Array[String]) {
    /* INITIALIZATION */
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    
    /* FUNCTIONS */
    // Extract "FromNodeId" and "Distance" and convert them into ("FromNodeId", "Distance_1") pair, the average distance of 1 outgoing node
    val array2pair1 = (p:Array[String]) => (p(1), p(3) + "_1")
    // Convert "AverageDistance" and "Count" into numbers
    val array2pair2 = (p:Array[String]) => (p(0).toDouble, p(1).toInt)
    // Calculate "AverageDistance_Count" based on two (AverageDistance, Count) pairs
    val average = (p1:(Double, Int), p2:(Double, Int)) => ((p1._1 * p1._2 + p2._1 * p2._2) / (p1._2 + p2._2)).toString + "_" + (p1._2 + p2._2).toString
    // Define ordering of the final result
    val order = new Ordering[(Int, Double)] {
      def compare(x:(Int, Double), y:(Int, Double)):Int = {
        var result = 0
        if (x._2 != y._2)
          result = -1 * (x._2 compare y._2) // sort descending based on AverageDistance
        else
          result = x._1 compare y._1        // sort ascending based on NodeId
        result
      }
    }
    
    /* CORE */
    val input = sc.textFile(inputFile)
    val pairs = input.map(line => array2pair1(line.split(" ")))
    val output = pairs.reduceByKey((s1, s2) => average(array2pair2(s1.split("_")), array2pair2(s2.split("_"))))
                      .map(s => (s._1.toInt, s._2.split("_")(0).toDouble))
                      .sortBy(p => p, true, 1)(order, classTag[(Int, Double)])
                      .map(s => s._1 + "\t" + s._2)
    output.saveAsTextFile(outputFolder)
  }
}