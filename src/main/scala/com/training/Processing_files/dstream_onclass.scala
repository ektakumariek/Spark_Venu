package com.training.Processing_files

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._



object dstream_onclass {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("streaming").getOrCreate()

    //val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    //val ssc = new StreamingContext(conf, Seconds(10))

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))






  }

}
