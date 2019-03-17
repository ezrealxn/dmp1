package com.doit.hunter.dmp.extra

import com.doit.hunter.dmp.utils.SessionUtil
import redis.clients.jedis.JedisPool

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/1 18:17
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:  利用Spark开发程序，将字典数据灌入redis
  **/
object LoadData2RedisSpark {

  def main(args: Array[String]): Unit = {

    val spark = SessionUtil.getSparkSession()

    val rdd = spark.sparkContext.textFile("g:/dmpdata/appdict/app_dict.txt")

    rdd.map(_.split("\t")).foreachPartition(iter=>{


      val jedis = new JedisPool().getResource

      iter.foreach(arr=>{

        jedis.hset("appdict",arr(4),arr(1))

      })

      jedis.close()

    })

    spark.close()

  }
}
