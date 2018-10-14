package com.training.dataframes

import org.apache.spark.sql.SparkSession

object UDF_during_class {

  def main(args: Array[String]): Unit = {

    val spark= SparkSession.builder().master("local[*]").appName("assign4").getOrCreate()

    val sc= spark.sparkContext


    val input1="file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/customers.csv"

    val df1= spark.read.format("csv").option("header","false").option("inferSchema","true").option("deliMeter",",").
      option("dateFormat","yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)



    df1.show()
  }

}
