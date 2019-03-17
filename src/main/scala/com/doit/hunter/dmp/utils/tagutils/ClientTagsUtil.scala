package com.doit.hunter.dmp.utils.tagutils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/2 16:13
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object ClientTagsUtil extends TagsUtil {


  def genTagsBrod(row: Row,dictMap:mutable.HashMap[String,String]): List[(String, Int)] = {

    var tags = List[(String, Int)]()

    var appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")

    val client = row.getAs[Int]("client")
    val networkmannerid = row.getAs[Int]("networkmannerid")
    val ispid = row.getAs[Int]("ispid")


    // 如果日志数据中的appname是空的，则需要去app字典数据中查找
    if (StringUtils.isEmpty(appname)) {
      val appnameTmp = dictMap.get(appid)
      appnameTmp match {
        case Some(name) => appname = name
        case None => appname = appid
      }
    }

    tags :+= ("APP"+appname,1)
    tags :+= ("D0001000"+client,1)
    tags :+= ("D0002000"+networkmannerid,1)
    tags :+= ("D0003000"+ispid,1)

    tags
  }

  override def genTags(row: Row): List[(String, Int)] ={

    List()
  }
}
