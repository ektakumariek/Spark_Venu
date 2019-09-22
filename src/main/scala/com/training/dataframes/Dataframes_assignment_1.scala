package com.training.dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.DateFormatClass
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import java.text.SimpleDateFormat

import java.util.Date


object Dataframes_assignment_1 {


  def main(args: Array[String]): Unit = {


    val spark= SparkSession.builder().master("local[*]").appName("Dataframe assignment").getOrCreate()

    val sc = spark.sparkContext //for rdd

    val sqlContext = spark.sqlContext //spark sql operations--for toDF()

    import sqlContext.implicits._




    val input1="file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/employees.csv"

    val df1= spark.read.format("csv").option("header","true").option("inferSchema","true").option("deliMeter",",").
     option("dateFormat","yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)

    val withoutSpechar=df1.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+",""))

      val newDf=df1.toDF(withoutSpechar:_*)

    val df2rdd=df1.rdd



    //val cleanData=df2rdd.map(x=> (x.trim().replaceAll("\"", "")).map(x => x.split(","))

    newDf.select($"LastName").show()




    //val removeSpChar= df

    //val newDf=toDF.()
    //val skip=input1.first()

    //val df2= input1.filter(x=>x!=skip).map(_.trim().replace("'","")).map(x=>x.split(",")).map(x=>(x(0),x(1)))
      //,x(2)
    //  ,x(3),x(4),x(5),x(6),x(7),x(8)

    //val dataFrame1 = spark.createDataFrame(df2,schema)



   // dataFrame1.show()

    //df.printSchema()






  /*//RDD way--how to apply case class structure to data


    val input1=sc.textFile("file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/employees.csv")

    val skip=input1.first()

    val df2= input1.filter(x=>x!=skip).map(_.trim().replace("'","")).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

    df2.printSchema()


   /* case class post(name:String,
                    n2:String,
                    name3:String,
                    name4:String,
                    name5:String,
                    startstamp6:TimestampType,
                    endsstamp:DateType,
                    name2:String,
                    name88:String)

    val postDf = spark.read.schema(schemaaa).csv("file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/employees.csv")
    //postDf.select($"name",$"")
    val ds = postDf.as[post]*/
*/


  }
}
