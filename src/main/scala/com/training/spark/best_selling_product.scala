package com.training.spark

import org.apache.spark.sql.SparkSession



object best_selling_product {

  def main(args: Array[String]): Unit = {

 val spark= SparkSession.builder.master("local[*]").appName("best_product").getOrCreate()

    val sc= spark.sparkContext

    val rdd= sc.textFile("file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/online-retail-dataset.csv")

    val skip= rdd.first()

    val filter1= rdd.filter(s=>s.contains("United Kingdom"))

    val rdd2 = filter1.filter(x=>x!=skip).map(_.trim).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7))).map(x=>(x._2,1))
      .reduceByKey((a,b)=>a+b).sortBy(x => x._2,false)



    //stock_code=(85123A,2215)-is the best selling,as it has been sold for max number of times
    rdd2.take(1).foreach(println)



 spark.stop()
  }
}
