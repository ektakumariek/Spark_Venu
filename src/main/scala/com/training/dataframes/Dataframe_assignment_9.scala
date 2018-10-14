package com.training.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object Dataframe_assignment_9 {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").appName("assign9").getOrCreate()

    /* 9)  from order-details dataset find which is the maximum profitable product name (not product id) to get proper results join with product dataset
    more info: in order-details u will get unit price and quantity multiply both and ... consider discount amount also to get exact result.*/

    val sc = spark.sparkContext

    val input1 = "file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/order-details.csv"


    //Logic to remove all special char from over the data including spaces.

    val df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("deliMeter", ",").
      option("dateFormat", "yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)

    val withNew= df1.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+",""))

    val newDF=df1.toDF(withNew:_*)

   newDF.show()


    val input2 = "file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/products.csv"

    val df2 = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("deliMeter", ",").
      option("dateFormat", "yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input2)

    val df2Name = df2.withColumnRenamed("_c0", "ProductID").withColumnRenamed("_c1", "ProductName").withColumnRenamed("_c2", "SupplierID").withColumnRenamed("_c3", "CategoryID").withColumnRenamed("_c4", "QuantityPerUnit").withColumnRenamed("_c5", "UnitPrice").withColumnRenamed("_c6", "UnitsInStock").withColumnRenamed("_c7", "UnitsOnOrder").withColumnRenamed("_c8", "ReorderLevel").withColumnRenamed("_c8", "Discontinued")

    df2Name.show()

    val joinDF = df2Name.join(newDF).where(newDF("ProductID") === df2Name("ProductID"))



    joinDF.show()

   val maxProfitable = joinDF.withColumn("MAX_PROFIT", (newDF.col("UnitPrice") - joinDF.col("Discount")) * joinDF.col("Quantity")).sort(desc("MAX_PROFIT"))

   // newjoinDF.show()
  }
}
