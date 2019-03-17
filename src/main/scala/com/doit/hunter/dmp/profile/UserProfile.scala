package com.doit.hunter.dmp.profile

import com.doit.hunter.dmp.bean.UserTagBean
import com.doit.hunter.dmp.utils.tagutils._
import com.doit.hunter.dmp.utils.{LocalDictDataUtil, SessionUtil, UserTagUtil}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/2 15:35
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description: 用户画像标签抽取
  **/
object UserProfile {

  def main(args: Array[String]): Unit = {


    val spark = SessionUtil.getSparkSession()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.read.parquet("g:/dmpdata/parquet2")

    val filteredDF = df
      .filter("imei !='' or  idfa!='' or  mac!='' or  androidid!='' or  openudid!='' " +
        "or  imeimd5!='' or  idfamd5!='' or  macmd5!='' or  androididmd5!='' or  openudidmd5!=''" +
        "or  imeisha1!='' or  idfasha1!='' or  macsha1!='' or  androididsha1!='' or  openudidsha1!=''")
      .filter(" cast(lng as double)> -180 and  cast(lng as double)<180  and cast(lat as double)> -90 and cast(lat as double) < 90 and (appname!='' or appid!='')")
      .select('imei, 'idfa, 'mac, 'androidid, 'openudid,
        'imeimd5, 'idfamd5, 'macmd5, 'androididmd5, 'openudidmd5,
        'imeisha1, 'idfasha1, 'macsha1, 'androididsha1, 'openudidsha1,
        'adspacetype, 'appname, 'appid, 'adplatformproviderid, 'client,
        'networkmannerid, 'ispid, 'provincename, 'cityname, 'keywords,'lng,'lat
      )


    // 加载appname字典数据，并广播
    val dictMap = LocalDictDataUtil.loadAppDict("g:/dmpdata/appdict/app_dict.txt")
    val dictBC = spark.sparkContext.broadcast(dictMap)

    // 加载stopwords字典数据，并广播
    val stopWdBC = spark.sparkContext.broadcast(LocalDictDataUtil.loadStopWords(""))


    // 抽取标签
    val tagsRDD: RDD[(String, List[(String, Int)])] = filteredDF.rdd.mapPartitions(iter=>{

      // 构造一个httpclient
      val httpClient = new HttpClient()
      // 构造一个jedis连接
      val jedis = new Jedis("c701",6379)

      val res=iter.map(
        row => {

          val imei = row.getAs[String]("imei")
          val idfa = row.getAs[String]("idfa")
          val mac = row.getAs[String]("mac")
          val androidid = row.getAs[String]("androidid")
          val openudid = row.getAs[String]("openudid")

          val imeimd5 = row.getAs[String]("imeimd5")
          val idfamd5 = row.getAs[String]("idfamd5")
          val macmd5 = row.getAs[String]("macmd5")
          val androididmd5 = row.getAs[String]("androididmd5")
          val openudidmd5 = row.getAs[String]("openudidmd5")

          val imeisha1 = row.getAs[String]("imeisha1")
          val idfasha1 = row.getAs[String]("idfasha1")
          val macsha1 = row.getAs[String]("macsha1")
          val androididsha1 = row.getAs[String]("androididsha1")
          val openudidsha1 = row.getAs[String]("openudidsha1")

          // 对每行抽取用户标识
          val uid: String = UserTagUtil.getUid(Array(
            imei,
            idfa,
            mac,
            androidid,
            openudid,
            imeimd5,
            idfamd5,
            macmd5,
            androididmd5,
            openudidmd5,
            imeisha1,
            idfasha1,
            macsha1,
            androididsha1,
            openudidsha1
          ))


          // 抽取地域属性标签（省，市）
          val areaTags = AreaTagsUtil.genTags(row)

          // 抽取用户客户端属性标签（app名称，网络类型，运营商，操作系统）
          val clientTags = ClientTagsUtil.genTagsBrod(row,dictBC.value)

          // 抽取用户页面关键字标签
          val kwTags = KeywordsTagsUtil.genTags(row,stopWdBC.value)

          // 抽取用户点击的广告栏位类型标签
          val adTags = AdTagsUtil.genTags(row)


          // TODO 商圈属性标签抽取（完善的流程： 先匹配本公司的商圈知识库，匹配不上再去请求高德的web服务
          val bizTags = BusinessTagsUtil.genTags(row,jedis,httpClient)


          (uid,areaTags ++ clientTags ++ kwTags ++ adTags ++ bizTags)
        }
      )
      jedis.close()
      res
    })

    // 将相同用户相同标签key的评分聚合
    // a,List(("APP爱奇艺",1),("DN00010001",1),("ZP湖北省",1))
    // b,List(("APP陌陌",1),("DN00010003",1),("ZP江西省",1))
    // a,List(("APP斗地主",1),("DN00010001",1),("ZP湖北省",1))
    // =>a,List(("APP爱奇艺",1),("APP斗地主",1),("DN00010001",2),("ZP湖北省",2))
    // =>b,List(("APP陌陌",1),("DN00010003",1),("ZP江西省",1))

    val resRDD: RDD[(String, List[(String, Int)])] = tagsRDD.reduceByKey((lst1, lst2)=>{

      //List(("APP爱奇艺",1),("DN00010001",1),("ZP湖北省",1))
      //List(("APP斗地主",1),("DN00010001",1),("ZP湖北省",1))

      // Map(ZP_hb -> 2, APP_aqy -> 1, DN00010001 -> 2, APP_ddz -> 1).toList
      (lst1 ++ lst2).groupBy(_._1).mapValues(_.size).toList
    })

    // 将结果RDD转换成DATAFRAME
    val resDF = resRDD.map(tp => {
      UserTagBean(tp._1, tp._2)
    }).toDF()



    resDF.coalesce(1).write.json("g:/dmpdata/usertags2")


    spark.close()
  }


}
