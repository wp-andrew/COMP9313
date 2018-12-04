package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SetSimJoin {  
  def find_pairs(key: Int, values: Iterable[(Int, Array[(Int, Int)])], T: Double, oT: Double) : scala.collection.mutable.Buffer[((Int, Int), Double)] = {
    val R = values.toList
    var out = scala.collection.mutable.Buffer[((Int, Int), Double)]()
    for (i <- 0 until R.length) {
      for (j <- (i + 1) until R.length) {
        // LENGTH FILTER
        if (R(j)._2.length >= T*R(i)._2.length && R(j)._2.length <= R(i)._2.length/T) {
          val rid1 = R(i)._1; val s0 = R(i)._2
          val rid2 = R(j)._1; val s1 = R(j)._2
          val mino = (oT * (s0.length + s1.length))  // minimum overlap
          var p0 = 0; var l0 = s0.length  // l0 = s0.length - p0
          var p1 = 0; var l1 = s1.length  // l1 = s1.length - p1
          var o  = 0  // current number of overlap or intersect
          var u  = 0  // current number of join or union
          // POSITIONAL FILTER
          while ((l0 > 0) && (l1 > 0) && (mino <= o + l0.min(l1))) {
            // if s0(p0) frequency is less than s1(p1) frequency
            if (s0(p0)._2 < s1(p1)._2)         {          u  += 1; p0 += 1         ; l0 -= 1          }
            // else if s0(p0) frequency is the same as s1(p1) frequency
            else if (s0(p0)._2 == s1(p1)._2) {
              // if s0(p0) value is less than s1(p1) value
              if (s0(p0)._1 < s1(p1)._1)       {          u  += 1; p0 += 1         ; l0 -= 1          }
              // else if s0(p0) value is the same as s1(p1) value
              else if (s0(p0)._1 == s1(p1)._1) { o  += 1; u  += 1; p0 += 1; p1 += 1; l0 -= 1; l1 -= 1 }
              // else s0(p0) value is higher than s1(p1) value
              else                             {          u  += 1;          p1 += 1;        ; l1 -= 1 }
            }
            // else s0(p0) frequency is higher than s1(p1) frequency
            else                               {          u  += 1;          p1 += 1;        ; l1 -= 1 }
          }
          u += (s0.length - p0 + s1.length - p1)
          val sim = o.toDouble / u
          if (sim >= T)
            out.append(((rid1, rid2), sim))
        }
      }
    }
    out
  }
  
  def main(args: Array[String]) {
    /* INITIALIZATION */
    val inputFile = args(0)
    val outputFolder = args(1)
    val T = args(2).toDouble
    val oT = T / (1 + T)
    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)
    
    /* CORE */
    val input = sc.textFile(inputFile)
    
    // Stage 1: Order Tokens by Frequency
    val freq = input.flatMap(line => line.split(" ").drop(1).map(eid => (eid.toInt, 1))).reduceByKey(_+_).collect().toMap
    val freqBC = sc.broadcast(freq)
    
    // Stage 2: Finding “similar” ID Pairs
    // PREFIX FILTER
    val ext_prefix = input.flatMap{ line => 
      val record = line.split(" ")
      val rid = record(0).toInt
      val s = record.drop(1).map(eid => (eid.toInt, freqBC.value.getOrElse(eid.toInt, -1)))
                            .sortBy{ case (eid, count) => (count, eid) }
      val prefix_length = (s.size - math.ceil(s.size*T) + 1).toInt
      val prefix = s.take(prefix_length)
      prefix.map{ case (eid, count) => (eid, (rid, s))}
    }.groupByKey()
    val sim_pairs = ext_prefix.flatMap{ case (key, values) => find_pairs(key, values, T, oT) }
    
    // Stage 3: Remove Duplicates and Sort
    val output = sim_pairs.reduceByKey{ case (sim1, sim2) => sim1 }
                          .sortByKey()
                          .map{ case (rids, sim) => rids + "\t" + sim }
    
    output.saveAsTextFile(outputFolder)
    freqBC.destroy()
  }
}