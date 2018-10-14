
package com.training.spark

object customer_spent_maximum_amount {

  def main(args: Array[String]): Unit = {


    import org.apache.spark.sql.SparkSession




    val spark = SparkSession.builder.master("local[*]").appName("customer_spent_maximum_amount").getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.textFile("file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/online-retail-dataset.csv")


    val skip = rdd.first()

    val rdd2 = rdd.filter(x=>x!=skip).map(_.trim).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3).toInt,x(4),x(5).toDouble,x(6),x(7)))


    val rdd3 =rdd2.map(x=>(x._7,x._4*x._6)).reduceByKey(_+_).sortBy(x=>x._2,false)

      //.filter(x=>x._1==15488)

    val skip2= rdd3.first()

    val rdd4= rdd3.filter(x=>x!=skip2)

    rdd4.take(1).foreach(println)

   //Ans is Customer_id= 14646


    spark.stop()
  }

}
