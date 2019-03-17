package com.doit.hunter.dmp.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/2 15:52
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object UserTagUtil {

  /**
    * 根据优先级，返回可用的用户标识
    * @param arr
    * @return
    */
  def getUid(arr: Array[String]): String = {


    var tmp = ""
    for (elem <- arr) {

      if (StringUtils.isNotEmpty(elem)) {
        tmp = elem
        return tmp
      }
    }

    tmp
  }

  def getUids(arr:Array[String]):Array[String] = {

    for (elem <- arr if StringUtils.isNotBlank(elem)) yield elem
  }




  def main(args: Array[String]): Unit = {
    println(getUid(Array("5", "2", "4", "3")))
  }
}