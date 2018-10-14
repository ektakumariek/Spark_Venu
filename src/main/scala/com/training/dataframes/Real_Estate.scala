package com.training.dataframes

import org.apache.spark.sql.SparkSession

object Real_Estate {

  def main(args: Array[String]): Unit = {


    //assgn ques want price based on bedroom and location

    val spark = SparkSession.builder().master("local[*]").appName("Real_Estate").getOrCreate()

    val sc = spark.sparkContext

    val input2 = "file:///Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/RealEstate.csv"

    val df2 = spark.read.format("csv").option("header", "True").load(input2)

    df2.createOrReplaceTempView("tab")

    val res=spark.sql("select Location, Bedrooms ,avg(Price_SQ_Ft) from tab group by Bedrooms,Location order by Location desc")

      res.show(100)

    df2.show()
  }
}
