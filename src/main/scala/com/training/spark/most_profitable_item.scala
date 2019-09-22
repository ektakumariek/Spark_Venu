package com.training.spark

object most_profitable_item {

  def main(args: Array[String]): Unit = {


    import org.apache.spark.sql.SparkSession


        val spark = SparkSession.builder.master("local[*]").appName("best_product").getOrCreate()

        val sc = spark.sparkContext

       val rdd = sc.textFile("file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/online-retail-dataset.csv")


        val skip = rdd.first()

     val rdd2 = rdd.filter(x=>x!=skip).map(_.trim).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3).toInt,x(4),x(5).toDouble,x(6),x(7)))


    val rdd3 =rdd2.map(x=>(x._2,x._4*x._6)).reduceByKey(_+_)
      //.sortBy(x=>x._2,false)


        rdd3.take(20).foreach(println)


        spark.stop()
      }

}
