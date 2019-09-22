package com.training.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Datafreme_assignment_3 {

  def main(args: Array[String]): Unit = {


    val spark =SparkSession.builder().master("local[*]").appName("ass3").getOrCreate()

    val sc=spark.sparkContext

    //assg ques:3) From categories and products dataset what is the maximum pfofitable product name and  CategoryName

    val input1="file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/categories_new.csv"

    val df1= spark.read.format("csv").option("header","false").option("inferSchema","true").option("deliMeter",",").
      option("dateFormat","yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)


    val df1Name=df1.withColumnRenamed("_c0","CategoryID").withColumnRenamed("_c1","CategoryName").withColumnRenamed("_c2","Description")



    val input2="file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/products.csv"

    val df2= spark.read.format("csv").option("header","false").option("inferSchema","true").option("deliMeter",",").
      option("dateFormat","yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input2)

    val df2Name=df2.withColumnRenamed("_c0","ProductID").withColumnRenamed("_c1","ProductName").withColumnRenamed("_c2","SupplierID").withColumnRenamed("_c3","CategoryID").withColumnRenamed("_c4","QuantityPerUnit").withColumnRenamed("_c5","UnitPrice").withColumnRenamed("_c6","UnitsInStock").withColumnRenamed("_c7","UnitsOnOrder").withColumnRenamed("_c8","ReorderLevel").withColumnRenamed("_c8","Discontinued")

    val joinDF= df1Name.join(df2Name).where(df1Name("CategoryID") ===
      df2Name("CategoryID"))

    joinDF.show()

    //val abc= joinDF.groupBy("CategoryName","ProductName").count().show()

    //abc.show()

    val newjoinDF=joinDF.withColumn("MAX_PROFIT",joinDF.col("UnitPrice") * joinDF.col("UnitsOnOrder")).sort(desc("MAX_PROFIT"))
      //.first()

   newjoinDF.show()
  }
}
