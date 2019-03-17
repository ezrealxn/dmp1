package com.doit.hunter.dmp.utils.tagutils
import org.apache.spark.sql.Row

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/2 16:12
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object AreaTagsUtil extends TagsUtil {
  override def genTags(row: Row): List[(String, Int)] = {
    var tags = List[(String, Int)]()

    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")

    tags :+= ("ZP"+provincename,1)
    tags :+= ("ZC"+cityname,1)

    tags
  }
}
