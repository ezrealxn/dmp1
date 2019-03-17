package com.doit.hunter.dmp.reports.area

import org.apache.spark.sql.SparkSession

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/27 14:35
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object KpiAreaDSL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("iloveyou").getOrCreate()


    //  读数据
    val df = spark.read.parquet("g:/dmpdata/parquet")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // snipaste 截图软件，神操作
    // 算子计算

    import org.apache.spark.sql.functions.substring


    val df2 = df.groupBy(substring($"requestdate",0,10),$"provincename",'cityname)
      .agg(
      sum(when('requestmode === 1 and 'processnode >= 1,1).otherwise(0)).as("rawrq")
      ,sum(when('requestmode === 1 and 'processnode >= 2,1).otherwise(0)).as("efrq")
      ,sum(when('requestmode === 1 and 'processnode === 3,1).otherwise(0)).as("adrq")
      ,sum(when('iseffective === 1 and 'isbilling === 1 and 'isbid === 1 and 'adorderid =!= 0,1).otherwise(0)).as("bidrq")
      ,sum(when('iseffective === 1 and 'isbilling === 1 and 'iswin === 1 ,1).otherwise(0)).as("winrq")
      ,sum(when('requestmode === 2 and 'iseffective === 1,1).otherwise(0)).as("adshow")
      ,sum(when('requestmode === 3 and 'iseffective === 1,1).otherwise(0)).as("clkrq")
      ,sum(when('iseffective === 1 and 'isbilling === 1 and 'iswin === 1 ,'winprice/1000d).otherwise(0.0)).as("price")
      ,sum(when('iseffective === 1 and 'isbilling === 1 and 'iswin === 1 ,'adpayment/1000d).otherwise(0.0)).as("payment")
    )

    df2.printSchema()
    df2.show(10,false)




    // 输出结果
    //df2.write.jdbc("","",new Properties())


    spark.close()





  }

}
