package com.doit.hunter.dmp.reports.app

import com.doit.hunter.dmp.utils.{KpiFlags, LocalDictDataUtil, SessionUtil}
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable
import scala.io.Source

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/1 15:11
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:   按照app维度（appname）来统计各个kpi指标
  **/
object KpiAppDimBroadCast {

  def main(args: Array[String]): Unit = {


    val appDict = LocalDictDataUtil.loadAppDict("g:/dmpdata/appdict/app_dict.txt")


    val spark = SessionUtil.getSparkSession()
    // 将app字典广播出去
    val bc = spark.sparkContext.broadcast(appDict)


    import spark.implicits._
    import org.apache.spark.sql.functions._


    val df1 = spark.read.parquet("g:/dmpdata/parquet")
    // 抽取，过滤数据
    val df2 = df1.select('requestdate, 'appname, 'appid, 'requestmode, 'processnode, 'iseffective, 'isbilling, 'isbid, 'iswin, 'winprice, 'adpayment, 'adorderid)
      .filter("appid !='' or appname!=''")


    // (12-20,爱奇艺),(是否原始请求，是否有效请求，是否参与竞价，是否竞价成功，是否展示，是否点击，广告消费，广告成本)
    // (12-20,爱奇艺),(1,0,1,1,1,0,0,1)
    // (12-20,爱奇艺),(1,1,0,0,1,1,1,0)
    df2.rdd.map(row => {

      val dt = row.getAs[String]("requestdate").substring(0, 10)

      var appname = row.getAs[String]("appname")
      val appid = row.getAs[String]("appid")

      // 如果日志数据中的appname是空的，则需要去app字典数据中查找
      if (StringUtils.isEmpty(appname)) {

        val appnameTmp = bc.value.get(appid)
        appnameTmp match {
          case Some(name) => appname = name
          case None => appname = appid
        }

      }

      val flags = KpiFlags.caculateKpi(row)


      ((dt, appname), flags)
    })
      .reduceByKey((lst1, lst2) => {
        lst1.zip(lst2).map(tp => tp._1 + tp._2)
      })


    // 输出到目标存储（可以写到文件、也可以写到mysql）


  }


}
