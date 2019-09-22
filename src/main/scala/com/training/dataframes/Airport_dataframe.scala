package com.training.dataframes

import com.training.dataframes.importExportOracle.Body_File
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object Airport_dataframe {

  case class Body_File(Airport_ID: String,
                       Airport_Name: String,
                       City_name: String,
                       Country: String,
                       FAA_code: String,
                       ICAO_Code: String,
                       Latitude: String,
                       Longitude: String,
                       Timezone: String,
                       DST: String,
                       Timezone_Olso: String
                      )

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").appName("airport").getOrCreate()

    val sc = spark.sparkContext

    val input1 = sc.textFile("file:///Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/airports.csv")

    import spark.implicits._

    val skip = input1.first()

    val newDF = input1.filter(x => x != skip).map(_.trim().replaceAll("\"", "")).map(x => x.split(",")).
      map(x => Body_File(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10))).toDF()

    //1) tell me top five airports countries.
    //2) store only United State and Canada country's records, with pipe (|) separated data.

    val top5Airport = newDF.groupBy("Country").count().orderBy(desc("count"))

    top5Airport.limit(5).show()

    newDF.createOrReplaceTempView("tmp1")

    val res = spark.sql("select * from (select country, count(country) cnt from tmp1  group by country order by cnt desc) res limit 5")


    newDF.show()

    res.show()

    val usAndCanadares = newDF.where($"Country" === ("United States") or $"Country" === ("Canada")).rdd


    usAndCanadares.map(row => row.mkString("|")).repartition(1).saveAsTextFile("file:///Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/Airport_output.csv")


  }
}
