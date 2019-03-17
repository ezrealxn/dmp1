package com.doit.hunter.dmp.extra

import redis.clients.jedis.JedisPool

import scala.io.Source

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/1 18:09
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:  单机模式读取app字典数据灌入redis
  **/
object LoadData2RedisLocal {

  def main(args: Array[String]): Unit = {

    val lines = Source.fromFile("g:/dmpdata/appdict/app_dict.txt").getLines()

    val pool = new JedisPool("c701",6379)
    val jedis = pool.getResource

    for (line <- lines) {

      val split = line.split("\t")
      val appId = split(4)
      val appName = split(1)

      // 将数据写入redis
      jedis.hset("appdict",appId,appName)
    }

    jedis.close()
    pool.close()
  }

}
