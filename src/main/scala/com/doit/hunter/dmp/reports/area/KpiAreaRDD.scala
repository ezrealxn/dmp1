package com.doit.hunter.dmp.reports.area

import java.util.Properties

import com.doit.hunter.dmp.bean.KpiAreaBean
import com.doit.hunter.dmp.utils.{KpiFlags, SessionUtil}
import org.apache.spark.rdd.RDD

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/27 17:16
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description: 请同学们完成RDD方式实现地域维度KPI指标计算
  **/


object KpiAreaRDD {

  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSparkSession()

    val df = spark.read.parquet("g:/dmpdata/parquet")

    val res: RDD[((String, String, String), List[Double])] = df.rdd.map(
      row => {

        // 取到日期
        val dt = row.getAs[String]("requestdate").substring(0, 10)
        val pname = row.getAs[String]("provincename")
        val cname = row.getAs[String]("cityname")

        val flags: List[Double] = KpiFlags.caculateKpi(row)

        // ((日,省,市),List(0,0,1,1,1,0,1,1,0))
        ((dt, pname, cname), flags)

      }
    ).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })

    import spark.implicits._
    val df_res = res.map(x => {
      KpiAreaBean(
        x._1._1,
        x._1._2,
        x._1._3,
        x._2(0).toInt,
        x._2(1).toInt,
        x._2(2).toInt,
        x._2(3).toInt,
        x._2(4).toInt,
        x._2(5).toInt,
        x._2(6).toInt,
        x._2(7),
        x._2(8)
      )
    }).toDF()


    df_res.write.jdbc("","",new Properties())

    spark.close()

  }

}
