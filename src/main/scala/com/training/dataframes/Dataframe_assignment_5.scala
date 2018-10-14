package com.training.dataframes

import org.apache.spark.sql.SparkSession

object Dataframe_assignment_5 {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local[*]").appName("ass5").getOrCreate()

    val sc=spark.sparkContext

    val input1="file:/Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/customers.csv"

    val df1= spark.read.format("csv").option("header","false").option("inferSchema","true").option("deliMeter",",").
      option("dateFormat","yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)


    val df1Name=df1.withColumnRenamed("_c0","CustomerID").withColumnRenamed("_c1","CompanyName").withColumnRenamed("_c2","ContactName").withColumnRenamed("_c3","ContactTitle").withColumnRenamed("_c4","Address").withColumnRenamed("_c5","City").withColumnRenamed("_c6","Region").withColumnRenamed("_c7","PostalCode").withColumnRenamed("_c8","Country")


    //5) from customers dataset, in region column replace null values set default value as others.

   // val new_df = df1Name.na.replace("Region",Map(""->0.0))
   // val new_df2 = df1Name.na.fill("e",Seq("blank"))



    val withoutSpechar=df1Name.columns.map(x=>x.replace("NULL","default"))

    val newDf=df1.toDF(withoutSpechar:_*)


    val newDf2=newDf.na.fill("e",Seq("NULL"))

    newDf2.show()



  }
}
