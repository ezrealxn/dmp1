package com.doit.hunter.dmp.utils.tagutils

import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/2 16:14
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object KeywordsTagsUtil extends TagsUtil {

  def genTags(row: Row, stopWords: mutable.HashMap[String, Null]): List[(String, Int)] = {
    var tags = List[(String, Int)]()

    val keywords = row.getAs[String]("keywords")

    keywords.split("\\|").filter(_.size > 2).filter(!stopWords.contains(_)).map(s => {
      tags :+= ("K" + s, 1)
    }
    )
    tags
  }

  override def genTags(row: Row): List[(String, Int)] = {

    List()
  }
}
