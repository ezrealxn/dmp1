package com.doit.hunter.dmp.reports.app

import java.util.Properties

import com.doit.hunter.dmp.bean.KpiAppBean
import com.doit.hunter.dmp.utils.{KpiFlags, SessionUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/1 18:25
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object KpiAppDimRedis {


  def main(args: Array[String]): Unit = {


    val spark = SessionUtil.getSparkSession()

    val df = spark.read.parquet("g:/dmpdata/parquet")

    import spark.implicits._
    import org.apache.spark.sql.functions._


    val df2 = df.select('requestdate, 'appname, 'appid, 'requestmode, 'processnode, 'iseffective, 'isbilling, 'isbid, 'iswin, 'winprice, 'adpayment, 'adorderid)
      .filter("appid !='' or appname!=''")


    // 计算逻辑从这里开始
    val resRDD: RDD[((String, String), List[Double])] = df2.rdd.mapPartitions(iter => {

      val jedis = new Jedis("c701", 6379)

      val res = iter.map(row => {

        val dt = row.getAs[String]("requestdate").substring(0, 10)

        var appname = row.getAs[String]("appname")
        val appid = row.getAs[String]("appid")

        // 如果日志数据中的appname是空的，则需要去app字典数据中查找
        if (StringUtils.isEmpty(appname)) {

          // 从redis中获取appname
          val appnameTmp = Option(jedis.hget("appdict", appid))

          appnameTmp match {
            case Some(name) => appname = name
            case None => appname = appid
          }

        }

        val flags = KpiFlags.caculateKpi(row)


        ((dt, appname), flags)
      })

      jedis.close()
      res

    }).reduceByKey((lst1, lst2) => {
      lst1.zip(lst2).map(tp => tp._1 + tp._2)
    })


    // 将 resRDD写入数据库
    val tmp: RDD[KpiAppBean] = resRDD.map {
      case ((dt, appname), List(a1, a2, a3, a4, a5, a6, a7, a8, a9)) => KpiAppBean(dt, appname, a1.toInt, a2.toInt, a3.toInt, a4.toInt, a5.toInt, a6.toInt, a7.toInt, a8, a9)
    }

    tmp.toDF().write.jdbc("", "", new Properties())


    spark.close()


  }

}
