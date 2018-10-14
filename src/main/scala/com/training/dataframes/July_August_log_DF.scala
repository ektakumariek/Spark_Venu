package com.training.dataframes

import com.training.dataframes.Airport_dataframe.Body_File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

object July_August_log_DF {

  case class Body_File(Airport_ID: String,
                       Airport_Name: String,
                       City_name: String,
                       Country: String,
                       FAA_code: String,
                       ICAO_Code: String,
                       Latitude: String
                      )

  def main(args: Array[String]): Unit = {

    val spark= SparkSession.builder().master("local[*]").appName("July_august").getOrCreate()

    val sc= spark.sparkContext


    val input1=sc.textFile("file:/Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/julydata.csv")

    //val df=spark.read.format("txt").option("delimiter"," ").option("inferschema","true").option("header","true")
      //.load(input1)

    import spark.implicits._

    val skip = input1.first()

    val newDF = input1.filter(x => x != skip).map(_.trim().replaceAll("\"", "")).map(x => x.split(",")).
      map(x => Body_File(x(0), x(1), x(2), x(3), x(4), x(5), x(6))).toDF()



    newDF.show(20)


  }

}
