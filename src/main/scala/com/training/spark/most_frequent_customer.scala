

package com.training.spark

import org.apache.spark.sql.SparkSession



object most_frequent_customer {

  def main(args: Array[String]): Unit = {

    val spark= SparkSession.builder.master("local[*]").appName("frequent_cust").getOrCreate()

    val sc= spark.sparkContext

    val rdd= sc.textFile("file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/online-retail-dataset.csv")

    //val rdd = sc.textFile("online-retail-dataset.csv")

    val skip= rdd.first()


    val rdd2 = rdd.filter(x=>x!=skip).map(_.trim).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7))).map(x=>(x._7,1)).
      reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)

    val skip2= rdd2.first()

    val rdd3= rdd2.filter(x=>x!=skip2)

    rdd3.take(10).foreach(println)

    //Answer is customer_id= 17841
    spark.stop()
  }
}
