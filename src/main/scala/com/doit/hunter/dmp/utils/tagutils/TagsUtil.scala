package com.doit.hunter.dmp.utils.tagutils

import org.apache.spark.sql.Row

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/2 16:10
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
trait TagsUtil {

  def genTags(row:Row): List[(String,Int)]

}
