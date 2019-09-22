package com.training.Processing_files

//this is kafka consumer code

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.serializer.KryoSerializer


import org.apache.spark.streaming._

object kafka_spark_sreaming_onclass {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("kafka_wordcount").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[2]").appName("kafka_wordcount").getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    val topics = Try(args(0).toString).getOrElse("new")
    val brokers = Try(args(1).toString).getOrElse("localhost:9092")
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "bootstrap.servers" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kaf",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))


    val lines = messages.map(_.value)
    /*val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()*/


    //import kafka.serializer.StringDecoder //kafka client jars

    //consumer -consuming data

    lines.foreachRDD { x =>
      if (x.isEmpty().equals(false)) {
        println(s"processing first line : $x")
        val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val df = x.map(x => x.split(" ")).map(x => (x(0), x(1), x(2))).toDF("name", "age", "city")
        df.show()
        //testing branch
        df.cache()
        /* df.cache()
       df.createOrReplaceTempView("tab")
       val hyddata = spark.sql("select * from tab where city='hyd'")
       val masdata = spark.sql("select * from tab where city='mas'")

       val oprop = new java.util.Properties()
       oprop.setProperty("user","ousername")
       oprop.setProperty("password","opassword")
       oprop.setProperty("driver","oracle.jdbc.OracleDriver")
       val url ="jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
       hyddata.write.mode(SaveMode.Append).jdbc(url,"hydinfo",oprop)
       masdata.write.mode(SaveMode.Append).jdbc(url,"masdata",oprop)
       df.printSchema()*/
      }
    }

    ssc.start() // Start the computation
    ssc.awaitTermination()
    //spark.stop()

  }
}



