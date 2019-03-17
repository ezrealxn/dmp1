package com.doit.hunter.dmp.reports.area

import org.apache.spark.sql.SparkSession

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/27 17:03
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object KpiAreaExpr {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("iloveyou").getOrCreate()


    //  读数据
    val df = spark.read.parquet("g:/dmpdata/parquet")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    df.select(substring('requestdate,0,10))

    // 使用expr 这个DS  api，可以写入sql语句
    df.select(substring($"requestmode",0,10))

    // selectExpr 本质上还是一个select算子，只能对单行处理，无法直接做聚合操作；
    // 而且，每一个列表达式，必须是一个单独的“”
    df.selectExpr("substr(requestmode,0,10)"  ,   ""    )

    // DLS中，要做聚合操作，还是得先调groupBy算子，然后调agg算子
    df.groupBy("","","").agg(sum($""),sum($""))


    spark.close()
  }

}
