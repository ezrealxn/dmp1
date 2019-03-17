package com.doit.hunter.dmp.utils

import scala.util.{Success, Try}

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/26 17:09
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description: 判断是否存在脏数据的工具类
  **/
object JudgeIfDirty {

  def isDirty(arr: Array[String]): Boolean = {


    // 输入一个字符串，判断它是否可以转成数字
    val isDouble = (s: String) => {
      Try(s.toDouble) match {
        case Success(i) => true
        case _ => false
      }
    }

    // 输入一个字符串，判断它是否可以转成数字
    val isInt = (s: String) => {
      Try(s.toInt) match {
        case Success(i) => true
        case _ => false
      }
    }


    isInt(arr(1)) &&
      isInt(arr(2)) &&
      isInt(arr(3)) &&
      isInt(arr(4)) &&
      isInt(arr(7)) &&
      isInt(arr(8)) &&
      isDouble(arr(9)) &&
      isDouble(arr(10)) &&
      isInt(arr(17)) &&
      isInt(arr(20)) &&
      isInt(arr(21)) &&
      isInt(arr(26)) &&
      isInt(arr(28)) &&
      isInt(arr(30)) &&
      isInt(arr(31)) &&
      isInt(arr(32)) &&
      isInt(arr(34)) &&
      isInt(arr(35)) &&
      isInt(arr(36)) &&
      isInt(arr(38)) &&
      isInt(arr(39)) &&
      isDouble(arr(40)) &&
      isDouble(arr(41)) &&
      isInt(arr(42)) &&
      isDouble(arr(44)) &&
      isDouble(arr(45)) &&
      isInt(arr(57)) &&
      isDouble(arr(58)) &&
      isInt(arr(59)) &&
      isInt(arr(60)) &&
      isInt(arr(73)) &&
      isDouble(arr(74)) &&
      isDouble(arr(75)) &&
      isDouble(arr(76)) &&
      isDouble(arr(77)) &&
      isDouble(arr(78)) &&
      isInt(arr(84))
  }


}
