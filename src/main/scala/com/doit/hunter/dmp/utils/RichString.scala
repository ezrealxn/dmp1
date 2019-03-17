package com.doit.hunter.dmp.utils

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/4 10:22
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
class RichString(s: String) {

  def toIntPlus: Int = {

    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }

  }




  def toDoublePlus: Double = {

    try {
      s.toDouble
    } catch {
      case e: Exception => 0d
    }

  }

}


object RichString{

  def apply(s: String): RichString = new RichString(s)

  implicit def str2RichString(s:String):RichString={

      RichString(s)

  }

}
