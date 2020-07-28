package com.edge

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._

object WordCount {
  def main(s: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("wordCount")
    val sc = new SparkContext(conf)
    val input = sc.textFile("data/wordCount.txt")
    val output = input.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
    output.foreach(println)
  }
}