package com.doit.hunter.dmp.utils.tagutils

import org.apache.spark.sql.Row

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/2 16:14
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object AdTagsUtil extends TagsUtil {
  override def genTags(row: Row): List[(String, Int)] = {

    var tags = List[(String, Int)]()

    val adspacetype = row.getAs[Int]("adspacetype")
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")

    if (adspacetype > 9) {

      tags :+= ("LC" + adspacetype, 1)
    } else {
      tags :+= ("LC0" + adspacetype, 1)
    }

    tags :+= ("CN"+adplatformproviderid,1)

    tags
  }
}
