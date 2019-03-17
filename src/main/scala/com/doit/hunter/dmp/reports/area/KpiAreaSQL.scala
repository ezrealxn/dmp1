package com.doit.hunter.dmp.reports.area

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/27 17:13
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object KpiAreaSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("iloveyou").getOrCreate()


    //  读数据
    val df = spark.read.parquet("g:/dmpdata/parquet")


    df.createTempView("dmp")

    val result = spark.sql(
      """
        select
        substr(requestdate,0,10) as dt,
        provincename,
        cityname,

        sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as raw_rq,
        sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as raw_rq,
        sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as raw_rq,
        sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as raw_rq,

        from dmp
        group by dt,provincename,cityname

      """.stripMargin)



    result.write.jdbc("","",new Properties())
  }

}
