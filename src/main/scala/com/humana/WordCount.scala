package com.humana

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
     
     val sc = new SparkContext(conf)
      val input=sc.textFile("C:\\Users\\mallikarjunarao\\Desktop\\input.txt")
     //Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))
   // words.foreach(println)
    
    // Count up the occurrences of each word
    val wordCounts = words.countByValue()
    
    // Print the results.
    wordCounts.foreach(println)
    
    
    //final result
  }
}