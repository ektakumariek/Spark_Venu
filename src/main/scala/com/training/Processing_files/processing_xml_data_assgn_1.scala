package com.training.Processing_files

import org.apache.spark.sql.SparkSession

object processing_xml_data_assgn_1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("assgn3").getOrCreate()

    val sc = spark.sparkContext

    val input2="file:///Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/ravi_kiran_trans00005.xml"

    val df2= spark.read.format("xml").option("rowTag","Transaction").load(input2)

    df2.createOrReplaceTempView("tab")

    val res=spark.sql(
      """select
        |BeginDateTime,BusinessDayDate,
        |t.UnitID.`_TypeCode` as TypeCode,
        |t.UnitID.`_VALUE` as VALUE,
        |CurrencyCode,EndDateTime,OperatorID.`_OperatorType` as op,OperatorID.`_VALUE` as Operatorvalue,ReceiptNumber,RetailStoreID,
        |r.BeginDateTime,
        |r.EndDateTime
        |,r.Sale.ActualSalesUnitPrice
        |,r.Sale.Associate.AssociateID.`_OperatorType` as OperatorType ,r.Sale.Associate.AssociateID.`_VALUE` as val,r.Sale.Description,r.Sale.ExtendedAmount
        |,it.`_Type` as itype
        |,it.`_VALUE` as ivalue
        |,r.Sale.ItemNotOnFileFlag
        |,r.Sale.MerchandiseHierarchy.`_Level` as Mlevel
        |,r.Sale.POSIdentity.POSItemID
        |,r.Sale.POSIdentity.`_POSIDType` as POSIDType
        |,r.Sale.Quantity.`_UnitOfMeasureCode` as UnitOfMeasureCode
        |,r.Sale.Quantity.`_VALUE` as Quan_Value
        |,r.Sale.RegularSalesUnitPrice
        |,r.Sale.SellingLocation.`_SubDepartmentCode` as SubDepartmentCode
        |,r.Sale.SellingLocation.`_VALUE` as VALUESEL
        |,r.Sale.Tax.Amount
        |,r.Sale.Tax.Percent
        |,r.Sale.Tax.SequenceNumber
        |,r.Sale.Tax.TaxAuthority
        |,r.Sale.Tax.TaxGroupID
        |,r.Sale.Tax.TaxRuleID
        |,r.Sale.Tax.TaxableAmount
        |,r.Sale.Tax.`_NegativeValue` as NegativeValue
        |,r.sale.Tax.`_TaxType` as TaxType
        |,r.Sale.Tax.pcms.`_TxnType` as TxnType
        |,r.Sale.TaxIncludedInPriceFlag
        |,r.Sale.UnitCostPrice
        |,r.Sale.`_ItemSubType` as ItemSubType
        |,r.Sale.`_ItemType` as ItemType
        |,r.Sale.`_NegativeValue` as NegativeValue

        |from tab
        |lateral view explode(BusinessUnit)b as t
        |lateral view explode(RetailTransaction.LineItem)c as r
        |lateral view explode(r.Sale.ItemID)d as it
        |lateral view explode(r.Total)e as to
        |""".stripMargin)

    res.show()

    res.printSchema()


    df2.printSchema()

    df2.show()

   /* ,r.Sale.it.`_Type` as itype ,r.Sale.it.`_VALUE` as ivalue
    ,r.Sale.ItemNotOnFileFlag,r.Sale.MerchandiseHierarchy.`_Level` as Mlevel,
    r.Sale.MerchandiseHierarchy.`_VALUE` as Mvalue*/
    //t._TypeCode as Typecode ,t._VALUE as Value
    //res.createOrReplaceTempView("tab1")

   // val res1= spark.sql("select BeginDateTime,BusinessDayDate,UnitID._TypeCode as TypeCode, UnitID._VALUE as VALUE from tab1 ")


    //oracle connectivity

    val url = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"

    val prop = new java.util.Properties()

    prop.setProperty("user", "ousername")

    prop.setProperty("password", "opassword")

    prop.setProperty("driver", "oracle.jdbc.OracleDriver")





    res.write.jdbc(url, "Ekta_Xll_Data", prop)







  }

}
