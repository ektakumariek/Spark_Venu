package com.training.dataframes

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object Dataframe_assgn_6_7_8 {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local[*]").appName("ass5").getOrCreate()

    val sc=spark.sparkContext

    val input1="file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/orders.csv"

    val df1= spark.read.format("csv").option("header","false").option("inferSchema","true").option("deliMeter",",").
      option("dateFormat","yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)


   /* 6) from orders dataset, find which (customer) ordered maximum products..

    7) from orders find which item took a lot of time to ship.
    more info: means shipped date ordered date difference consider as delayed date

      8) from orders dataset  find from which country maximum orders ordered? */


    val df1Name=df1.withColumnRenamed("_c0","OrderID").withColumnRenamed("_c1","CustomerID").withColumnRenamed("_c2","EmployeeID").withColumnRenamed("_c3","OrderDate").withColumnRenamed("_c4","RequiredDate").withColumnRenamed("_c5","ShippedDate").withColumnRenamed("_c6","ShipVia").withColumnRenamed("_c7","Freight").withColumnRenamed("_c8","ShipName").withColumnRenamed("_c9","ShipAddress").withColumnRenamed("_c10","ShipCity").withColumnRenamed("_c11","ShipRegion").withColumnRenamed("_c12","ShipPostalCode").withColumnRenamed("_c13","ShipCountry")

    val dateChange= df1Name.withColumn("OrderDate",df1Name("OrderDate").cast("date")).withColumn("ShippedDate",df1Name("ShippedDate").cast("date"))

    dateChange.printSchema()

    dateChange.show()

    df1Name.printSchema()

    val newDf= df1Name.groupBy("CustomerID").count()

    val ordMax= newDf.sort(desc("count"))

    ordMax.first()

    //ordMax.show()

    df1Name.show()

    df1Name.printSchema()

    val maxShipTime=dateChange.withColumn("Max_shipping_time",dateChange.col("OrderDate") - dateChange.col("ShippedDate"))

    //convert string to date before substraction



    maxShipTime.sort(desc("Max_shipping_time")).show()

    val ordMaxCountry= df1Name.groupBy("ShipCountry").count()

    val ordMaxCountrySort= ordMaxCountry.sort(asc("count"))

   // ordMaxCountrySort.first()

    ordMaxCountrySort.show()



  }

}
