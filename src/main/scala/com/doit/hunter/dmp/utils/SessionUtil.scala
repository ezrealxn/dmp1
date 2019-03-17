package com.doit.hunter.dmp.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/26 16:49
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description: spark连接创建工具
  **/
object SessionUtil {

  def getSparkSession(options:Map[String,String]=Map(),appName:String = "dmp",master:String = "local[*]") ={
    val conf = new SparkConf()
    // TODO  把options中的参数传入conf


    SparkSession.builder().appName(appName).master(master).getOrCreate()
  }

}
