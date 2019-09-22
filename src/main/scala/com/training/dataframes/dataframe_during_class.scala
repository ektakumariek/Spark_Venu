package com.training.dataframes


import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf

import org.apache.spark.sql._




object dataframe_during_class {

  //case class demo ()


    def main(args: Array[String]): Unit = {


      val spark = SparkSession.builder.master("local[*]").appName("best_product").getOrCreate()



      //val sc = spark.sparkContext //for rdd

      //val sqlContext = spark.sqlContext //spark sql operations

      //import sqlContext.implicits._


      //val rdd = sc.textFile("file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/online-retail-dataset.csv")


      //val skip = rdd.first()

      //RDD WAY TO CREATE DF

      //val rdd2 = rdd.filter(x=>x!=skip).map(_.trim).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3).toInt,x(4),x(5).toDouble,x(6),x(7))).toDF()

      //READ METHOD to CREATE RDD

     //val input1="file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/online-retail-dataset.csv"

      val input2="file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/asl.txt"

      val input3="file:/Users/ekumari/Desktop/Ekta folder/Venu Class/Dataset/nep.txt"

      val df = spark.read.format("csv").option("header","false").option("inferSchema","true").load(input2)

      val df1 = spark.read.format("csv").option("header","false").option("inferSchema","true").load(input3)

     //df.select ("InvoiceNo","Country").show()

      //df.select("Country").distinct().show()



      //df.show()


  //df.printSchema()

      //df.head(4)

      //df.limit(5)

      val dffname1=df.withColumnRenamed("_c0","Name").withColumnRenamed("_c1","age").withColumnRenamed("_c2","city")

      val dffname2=df1.withColumnRenamed("_c0","Name").withColumnRenamed("_c1","mail_id").withColumnRenamed("_c2","mobile_number")

      //dffname1.show()

     // dffname1.select($"Name",substring($"Name",1,2).alias("namesub")).show()
     // dffname1.select($"Name",translate($"age","23","50").alias("new_age")).show()
     dffname1.show()
      //dffname1.select($"Name",substring_index($"Name","1",3).alias("index")).show()
      //-----chk this
     //dffname1.select("Name","age").show()

      //dffname2.select($"Name" , $"mail_id").show()

     // dffname1.select(col("Name")).show()

      //dffname1.select(col("Name")==="Venu").count()

      dffname1.createOrReplaceTempView("sqlTable1")

     // dffname2.createOrReplaceTempView("sqlTable2")

      //dffname1.where(col("age")>"23").show()

      //dffname1.filter(col("age")==="23").count()

      //dffname1.filter("age=23").count()

      //dffname1.drop("Name").show()



     // dffname1.select (col("Name")).distinct.show()

      //dffname1.dropDuplicates().show()

      //dffname1.groupBy("Name").sum("age").show()

       //dffname1.explain()

      //dffname1.select(col("Name"),col("age")).orderBy("Name").show()

      //dffname1.select(col("Name"),col("age")).sort("Name").show()

      //dffname1.sort(col("age")).show()

      //dffname1.explode(col("city")) ----Check on exxplode

      //dffname1.show()

     // dffname1.count()

      //dffname2.show()


      //val sqldf=  spark.sql("select sqlTable1.age,sqlTable2.mail_id from sqlTable1 inner join sqlTable2 on sqlTable1.Name=sqlTable2.Name")

      dffname1.show()

     // val sqldf=  spark.sql("select age, case when (age%2==0)=True then even_age else old_age end  from sqlTable1")


      //sqldf.show()
     //dffname1.show()
     // dffname2.show()
      //val rdd3 = spark.createDataFrame(rdd2)
      //rdd3.show(5)
      //val df= rdd2.toDF()


      //val rdd3 =rdd2.map(x=>(x._2,x._4*x._6)).reduceByKey(_+_)
      //.sortBy(x=>x._2,false)

        //val xyz = spark.sparkContext.parallelize(Array(1,"xyz"))
      //val with_df = xyz.toDF
      //df.take(20).foreach(println)


      spark.stop()
    }

  }



