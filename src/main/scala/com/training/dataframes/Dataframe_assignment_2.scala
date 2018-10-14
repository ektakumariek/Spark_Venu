package com.training.dataframes

import org.apache.spark.sql.SparkSession

object Dataframe_assignment_2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("ass2").getOrCreate()

    val sc = spark.sparkContext

    val input1 = "file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/suppliers.csv"

    val df1 = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("deliMeter", ",").
      option("dateFormat", "yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)

    val df2 = df1.withColumnRenamed("_c0", "SupplierID").withColumnRenamed("_c1", "CompanyName").withColumnRenamed("_c2", "ContactName").withColumnRenamed("_c3", "ContactTitle").withColumnRenamed("_c4", "Address").withColumnRenamed("_c5", "City").withColumnRenamed("_c6", "Region").withColumnRenamed("_c7", "PostalCode")

    //from supppliers dataset get  productname and supplierid who is in 'Exotic Liquids', 'Grandma Kelly''s Homestead', 'Tokyo Traders' AND ProductID must be 14

    df2.where("SupplierID=14").show()


    // val rdd2= dftordd.map(x=>(x(0),x(1))).map(_.trim)
    //df2.select("CompanyName".toInt).show()

    //df2.show()
  }
}


/*questions

1.How to convert DF to rdd

2. How to remove single quotes from data in dataframe

3. How to remove header in dataframe

4. what are dataframena function
*/
