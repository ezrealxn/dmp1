package com.doit.hunter.dmp.utils

import scala.collection.mutable
import scala.io.Source

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/2 15:48
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object LocalDictDataUtil {

  /**
    * 加载appname和appid之间映射关系的方法
    * @param path
    * @return
    */
  def loadAppDict(path:String)={
    val source = Source.fromFile(path).getLines()
    val appDict = new mutable.HashMap[String, String]()
    for (line <- source) {
      val split = line.split("\t", -1)
      appDict.put(split(4), split(1))
    }

    appDict

  }


  /**
    * 加载停止词字典
    * @param path
    * @return
    */
  def loadStopWords(path:String):mutable.HashMap[String,Null]={

    // TODO 加载停止词字典文件
    val map = new mutable.HashMap[String,Null]()

    map += ("法轮大法"-> null,"崔永元"->null)


  }


}
