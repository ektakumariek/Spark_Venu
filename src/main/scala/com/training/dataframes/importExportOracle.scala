package com.training.dataframes


import org.apache.spark.sql.SparkSession



object importExportOracle {


  case class Body_File(first_name: String,
                       last_name: String,
                       company_name: String,
                       address: String,
                       city: String,
                       county: String,
                       state: String,
                       zip: String,
                       phone1: String,
                       phone2: String,
                       email: String,
                       web: String)


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("ass3").getOrCreate()

    val sc = spark.sparkContext


    val input1 = sc.textFile("file:///Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/us-500.csv")

    import spark.implicits._

    val skip = input1.first()

    val newDF = input1.filter(x => x != skip).map(_.trim().replaceAll("\"", "")).map(x => x.split(",")).map(x => Body_File(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11))).toDF()


    newDF.show()


    val url = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"

    val prop = new java.util.Properties()

    prop.setProperty("user", "ousername")

    prop.setProperty("password", "opassword")

    prop.setProperty("driver", "oracle.jdbc.OracleDriver")


    newDF.createOrReplaceTempView("tmp1")

    val query = spark.sql("select * from tmp1")

    query.write.jdbc(url, "Ekta_case_class", prop)

    spark.stop()

  }
}

    //val oracleDF2=spark.createDataFrame(Body_File,input1)

    //createDataFrame(input1,Body)


    //val cleanDataRdd=newDF.columns.map(x=>x.replaceAll("^\\\"|\\\"$",""))

    // val cleanDF=newDF.withColumn("company_name_yyyy",regexp_replace(col("company_name"),"^\"|\"$",""))

    //val cleanDF = newDF.select($"company_name",translate($"company_name",$"   ","piyush"))
    //cleanDF.show(false)

    //  val cleanFD=newDF.toDF()

    // cleanDF.show()

    //importing based on custom query for selected sal

    //val query1 = "(select * from emp where sal > 2000) tmp"

    //val df2 = spark.read.jdbc(url, query1, prop)

    //importing based on dataframe

    //val df3= spark.read.format("csv").option("header","false").option("inferSchema","true").option("deliMeter",",").
     // option("dateFormat","yyyy-MM-dd HH:mm:ss").option("escape", "'").load(input1)

   // val dfnew = spark.read.jdbc(url,query,prop)

    //dfnew.show()

    //import all tables ,should be present either in oracle or td or sql and add related depency also in project

    //val tables=Array("emp","deept","stud")

    //val query = "(select table_name from all_tables where TABLESPACE_NAME='USERS') tmptst"

    //val url = "jdbc:oracle:thin://@oracledb.ctx5qjxp7j6n.ap-south-1.rds.amazonaws.com:1521/ORCL"

    //val prop = new java.util.Properties()

    //prop.setProperty("user", "ousername")

    //prop.setProperty("password", "opassword")

    //prop.setProperty("driver", "oracle.jdbc.OracleDriver")




    //val alltab = df.select("TABLE_NAME").rdd.map(r => r(0)).collect.toList

    //alltab.foreach{x=>

      //val table=x.toString()

    //val url="jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"

    //val prop = new java.util.Properties()

    //prop.setProperty("user","ousername")

    //prop.setProperty("password","opassword")

    //prop.setProperty("driver","oracle.jdbc.OracleDriver")


      //val dftable = spark.read.jdbc(url, table, prop)

      //dftable.write.format("orc").saveAsTable("table")







    //df.show()

   // spark.stop()

   // val prop= new java.utils.p



