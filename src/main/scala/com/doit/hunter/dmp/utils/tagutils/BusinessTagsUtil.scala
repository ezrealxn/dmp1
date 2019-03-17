package com.doit.hunter.dmp.utils.tagutils
import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/4 9:10
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object BusinessTagsUtil {
  def genTags(row: Row,jedis:Jedis,httpClient:HttpClient): List[(String, Int)] = {


    val tagList = new ListBuffer[(String,Int)]

    val lat = row.getAs[String]("lat")
    val lng = row.getAs[String]("lng")

    // 将经纬度数据转换成geohash编码，去本地商圈知识库查询商圈信息
    val geocode  = GeoHash.withCharacterPrecision(lat.toDouble,lng.toDouble,9).toBase32


    // 如果本地查询不到，则请求高德地图服务获取商圈信息      we124ds0--> 故宫,景山公园
    val bizInfos = jedis.hget("bizdict",geocode)
    if(bizInfos != null){
      val tmp= bizInfos.split(",").map((_,1)).toList
      tagList ++ tmp
    }else {
      // 如果高德地图服务获取商圈信息成功，则将商圈信息存入本地知识库
      val method = new GetMethod("https://restapi.amap.com/v3/geocode/regeo?key=e384b6b8bc2f8e9e9e92a9cf969da45c&location="+lng+","+lat)

      httpClient.executeMethod(method)

      val resJson = method.getResponseBodyAsString
      val rootObj: JSONObject = JSON.parseObject(resJson)

      val status = rootObj.getString("status")
      if("1".equals(status)) {

        val bizAreas: JSONArray = rootObj.getJSONObject("regeocode").getJSONObject("addressComponent").getJSONArray("businessAreas")

        val sb = new StringBuilder
        if (bizAreas != null) {
        for (i <- 0 until bizAreas.size()) {
          val bizName = bizAreas.getJSONObject(i).getString("name")
          sb.append(bizName)
          tagList += ((bizName, 1))
        }
      }
        jedis.hset("bizdict",geocode,sb.mkString(","))

      }
      // 释放http连接
      method.releaseConnection()

    }
    // 返回标签
    tagList.toList
  }
}
