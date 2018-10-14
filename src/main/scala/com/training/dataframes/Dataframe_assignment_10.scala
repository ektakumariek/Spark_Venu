package com.training.dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Dataframe_assignment_10 {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local[*]").appName("assgn10").getOrCreate()

    val sc=spark.sparkContext

    val input1="file:/Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/employees.csv"

    val df1= spark.read.format("csv").option("header","true").option("inferSchema","true").option("deliMeter",",").
      option("dateFormat","yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)

    val cleanHeader= df1.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+",""))

    val cleanedDf=df1.toDF(cleanHeader:_*)

    //assgn 10

    //from employee data find who is youngest employee

    val sortData= cleanedDf.orderBy(asc("HireDate"))

    sortData.show()

    //oldest employee as per joining date tell me their details

    val finaldata =sortData.orderBy(desc("BirthDate"))

    finaldata.show()


  }

}
