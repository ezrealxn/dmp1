package com.doit.hunter.dmp.reports

import java.util.Properties

import com.doit.hunter.dmp.utils.SessionUtil
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/26 17:42
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:  省市数据分布概况
  **/
object ProvinceCityOverView {


  def main(args: Array[String]): Unit = {

    val spark = SessionUtil.getSparkSession(Map())


    val df = spark.read.parquet("g:/dmpdata/parquet")
    // DATASET api方式
    val result: DataFrame = df.groupBy("provincename","cityname").count()

    // TODO  sql 方式



    // TODO  rdd 方式


    // TODO  连接属性从配置文件获取
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root")

    // 将结果数据写入mysql数据库
    result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/dmp?characterEncoding=utf8","pro_city_ov",props)


    // 将结果数据写成json，存入文件系统
    result.coalesce(2).write.json("g:/dmpdata/jsonout/")


    spark.close()


  }


}
