package com.training.Processing_files

import org.apache.spark.sql.SparkSession

object processing_xml_data_assgn_1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("assgn3").config("spark.debug.maxToStringFields", "100").
      getOrCreate()

    val sc = spark.sparkContext

    val input2 = "file:///Users/ekumari/Desktop/Ektafolder/Venu Class/Dataset/ravi_kiran_trans00005.xml"

    val df2 = spark.read.format("xml").option("rowTag", "Transaction").load(input2)

    df2.createOrReplaceTempView("tab")

    val res = spark.sql(
      """select
        |BeginDateTime,BusinessDayDate,
        |t.UnitID.`_TypeCode` as TypeCode,
        |t.UnitID.`_VALUE` as VALUE,
        |CurrencyCode,EndDateTime,OperatorID.`_OperatorType` as op,OperatorID.`_VALUE` as Operatorvalue,ReceiptNumber,RetailStoreID,
        |r.BeginDateTime as Begin_time,
        |r.EndDateTime as End_time
        |,r.Sale.ActualSalesUnitPrice
        |,r.Sale.Associate.AssociateID.`_OperatorType` as OperatorType ,r.Sale.Associate.AssociateID.`_VALUE` as val,r.Sale.Description,r.Sale.ExtendedAmount
        |,it.`_Type` as itype
        |,it.`_VALUE` as ivalue
        |,IF(r.Sale.ItemNotOnFileFlag=1,'TRUE','FALSE') as ItemNotonFIle
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
        |,r.Sale.Tax.SequenceNumber as seq_nbr
        |,r.Sale.Tax.TaxAuthority
        |,r.Sale.Tax.TaxGroupID
        |,r.Sale.Tax.TaxRuleID
        |,r.Sale.Tax.TaxableAmount
        |,IF(r.Sale.Tax.`_NegativeValue`=1,'TRUE','FALSE') as Tax_NegativeValue
        |,r.sale.Tax.`_TaxType` as TaxType
        |,r.Sale.Tax.`pcms:PCMSTax`.`pcms:DetailNumber`.`_TxnType` as TxnType
        |,r.Sale.Tax.`pcms:PCMSTax`.`pcms:DetailNumber`.`_VALUE` as pcms_value
        |,IF(r.Sale.TaxIncludedInPriceFlag=1,"TRUE","FALSE") as TaxIncludedInPrice
        |,r.Sale.UnitCostPrice
        |,r.Sale.`_ItemSubType` as ItemSubType
        |,r.Sale.`_ItemType` as ItemType
        |,IF(r.Sale.`_NegativeValue`=1,"TRUE","FALSE") as NegativeValue
        |,r.Sale.`pcms:PCMSSale`.`pcms:ExtendedAmountAllDiscounts` as pcms_extendeddiscount
        |,r.Sale.`pcms:PCMSSale`.`pcms:LinkedItemLevel` as pcms_LinkedItemLevel
        |,r.Sale.`pcms:PCMSSale`.`pcms:MerchandisingFormat` as MerchandisingFormat
        |,r.Sale.`pcms:PCMSSale`.`pcms:OwningDepartment` as OwningDepartment
        |,r.Sale.`pcms:PCMSSale`.`pcms:PriceControl` as PriceControl
        |,r.Sale.`pcms:PCMSSale`.`pcms:PriceDiscountable` as PriceDiscountable
        |,r.Sale.`pcms:PCMSSale`.`pcms:ProductHandling` as ProductHandling
        |,IF(r.Sale.`pcms:PCMSSale`.`pcms:Resaleable`=1,"TRUE","FALSE") as Resaleable
        |,r.Sale.`pcms:PCMSSale`.`pcms:SelfScanPrice` as SelfScanPrice
        |,r.SequenceNumber as seq
        |,r.Tender.Amount as Tender_amt
        |,r.Tender.TenderChange.Amount as Ten_Change_amt
        |,r.Tender.TenderChange.TenderID
        |,r.Tender.TenderChange.`_TenderType` as TenderType
        |,r.Tender.TenderChange.`pcms:PCMSTender`.`pcms:MediaType` as MediaType1
        |,r.Tender.TenderID as Tend_id
        |,r.Tender.`_TenderType`
        |,r.Tender.`_TypeCode`
        |,r.Tender.`pcms:PCMSTender`.`pcms:MediaType` as MediaType2
        |,r.`_EntryMethod`
        |,r.`_LineType`
        |,r.`pcms:PCMSCustomData`.`_Name` as pcms_name
        |,r.`pcms:PCMSCustomData`.`_VALUE` as pcms_value_CustomData
        |,r.`pcms:PCMSLineItem`.`pcms:DetailNumber`.`_TxnType`[0] as txn_type
        |,r.`pcms:PCMSLineItem`.`pcms:DetailNumber`.`_VALUE`[0] as VALUE_Detail
        |,r.`pcms:PCMSRefusedSale`.Reason as Reason
        |,RetailTransaction.LoyaltyAccount.CustomerID
        |,to.`_TotalType`
        |,to.`_VALUE`
        |,IF(RetailTransaction.`_OutsideSalesFlag`=1,"TRUE","FALSE") as OutsideSalesFlag
        |,RetailTransaction.`_TransactionStatus`
        |,RetailTransaction.`_Version`
        |,RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`.`pcms:CustomerType`[0] as CustomerType
        |,IF(RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`.`pcms:DeclinedEmail`[0]=1,"TRUE","FALSE") as DeclinedEmail
        |,RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`.`pcms:EmailAddress`[0] as EmailAddress
        |,RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`.`pcms:LoyaltyAccountID`[0] as LoyaltyAccountID
        |,RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`.`pcms:LoyaltyAccountStatus`[0] as LoyaltyAccountStatus
        |,RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`.`pcms:LoyaltyCaptureMode`[0] as LoyaltyCaptureMode
        |,RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`.`pcms:LoyaltyCardType`[0] as LoyaltyCardType
        |,RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`.`pcms:LoyaltyProgramName`[0] as LoyaltyProgramName
        |,IF(RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`.`pcms:UpdatedEmail`[0]=1,"True","False") as UpdatedEmail
        |,SequenceNumber as s_nbr
        |,WorkstationID
        |,`pcms:PCMSTransaction`.`pcms:BuildNumber` as builder
        |,`pcms:PCMSTransaction`.`pcms:DBSchemaVersion` as schema
        |,`pcms:PCMSTransaction`.`pcms:ExternalSystem`  as external
        |,`pcms:PCMSTransaction`.`pcms:OnlineStatus` as onlinestatus
        |,`pcms:PCMSTransaction`.`pcms:TransactionType` as transaction
        |,`pcms:PCMSTransaction`.`pcms:UTCOffset` as utc
        |,`pcms:PCMSTransaction`.`pcms:WorkstationPersonality` as work
        |from tab
        |lateral view explode(BusinessUnit)b as t
        |lateral view explode(RetailTransaction.LineItem)c as r
        |lateral view explode(r.Sale.ItemID)d as it
        |lateral view explode(RetailTransaction.Total)e as to
        |""".stripMargin)

    res.show()

    res.printSchema()

    //oracle connectivity

    val url = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"

   val prop = new java.util.Properties()

    prop.setProperty("user", "ousername")

    prop.setProperty("password", "opassword")

    prop.setProperty("driver", "oracle.jdbc.OracleDriver")


    res.write.mode("overwrite").jdbc(url, "Ekta_XMl_Data", prop)


  }

}
