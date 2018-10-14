package com.training.dataframes

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object Dataframe_assignment_4 {

  def main(args: Array[String]): Unit = {

    //from customers dataset, find from which country maximum customers coming?

    val spark= SparkSession.builder().master("local[*]").appName("assign4").getOrCreate()

    val sc= spark.sparkContext

    //from customers dataset, find from which country maximum customers coming?

    val input1="file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/customers.csv"

    val df1= spark.read.format("csv").option("header","false").option("inferSchema","true").option("deliMeter",",").
      option("dateFormat","yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)



    val df1Name=df1.withColumnRenamed("_c0","CustomerID").withColumnRenamed("_c1","CompanyName").withColumnRenamed("_c2","ContactName").withColumnRenamed("_c3","ContactTitle").withColumnRenamed("_c4","Address").withColumnRenamed("_c5","City").withColumnRenamed("_c6","Region").withColumnRenamed("_c7","PostalCode").withColumnRenamed("_c8","Country")

    df1Name.show()

    val dfGrp= df1Name.groupBy("Country").count().as("ss")



    dfGrp.sort(desc("count")).show()

  }

}
