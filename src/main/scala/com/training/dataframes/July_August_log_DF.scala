package com.training.dataframes
import org.apache.spark.sql.functions._
import com.training.dataframes.Airport_dataframe.Body_File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

object July_August_log_DF {

  case class Body_File(host: String,
                       logname: String,
                       time: String,
                       method: String,
                       url: String,
                       response: String,
                       bytes: String
                      )



  def main(args: Array[String]): Unit = {

    val spark= SparkSession.builder().master("local[*]").appName("July_august").getOrCreate()

    val sc= spark.sparkContext


    val input1=sc.textFile("file:/Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/julydata.csv")

    val input2=sc.textFile("file:/Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/augdata.csv")

    //val df=spark.read.format("txt").option("delimiter"," ").option("inferschema","true").option("header","true")
      //.load(input1)

    import spark.implicits._
// from rdd/datraframe skip first line

    val skip = input1.first()

    // here replace all remove all special symbols.  body fse class define din 9 line

    val newDFJuly = input1.filter(x => x != skip).map(_.trim().replaceAll("\"", "")).map(x => x.split(",")).
      map(x => Body_File(x(0), x(1), x(2), x(3), x(4), x(5), x(6))).toDF()

    val newDFAugust = input2.filter(x => x != skip).map(_.trim().replaceAll("\"", "")).map(x => x.split(",")).
      map(x => Body_File(x(0), x(1), x(2), x(3), x(4), x(5), x(6))).toDF()

    //1.Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days. pls find july and aug data

    //  Create a Spark program to generate a new RDD which contains the log lines from both July  and August  without any duplicates

    newDFAugust.show()


    val joinDf= newDFJuly.join(newDFAugust).where(newDFAugust("method") === newDFJuly("method"))

    joinDf.show()


    //joinDf.show()

    //val ss=newDFAugust.where(col("host") === "in24.inetnebr.com")

   //ss.show()


    newDFAugust.createOrReplaceTempView("tab1")
    newDFJuly.show()

    newDFJuly.createOrReplaceTempView("tab2")

    newDFAugust.select(col("host")).show()
   //val joinn = newDFJuly.join(newDFAugust,newDFJuly("method")===newDFAugust("method"))

   //joinn.show()

    //val res=spark.sql("select tab1.host from tab1 inner join tab2 on tab1.host=tab2.host")

    //res.show()





    newDFJuly.show(20)


    newDFAugust.show()



  }

}
