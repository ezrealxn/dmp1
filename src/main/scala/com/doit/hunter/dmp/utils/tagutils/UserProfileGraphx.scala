package com.doit.hunter.dmp.utils.tagutils

import com.doit.hunter.dmp.utils.{LocalDictDataUtil, SessionUtil, UserTagUtil}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/3 14:34
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:   画像标签按唯一用户标识聚合
  **/
object UserProfileGraphx {

  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSparkSession()

    // 加载appname字典数据，并广播
    val dictMap = LocalDictDataUtil.loadAppDict("g:/dmpdata/appdict/app_dict.txt")
    val dictBC = spark.sparkContext.broadcast(dictMap)

    // 加载stopwords字典数据，并广播
    val stopWdBC = spark.sparkContext.broadcast(LocalDictDataUtil.loadStopWords(""))


    val df = spark.read.parquet("g:/dmpdata/parquet")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val filteredDF = df
      .select('imei, 'idfa, 'mac, 'androidid, 'openudid,
      'imeimd5, 'idfamd5, 'macmd5, 'androididmd5, 'openudidmd5,
      'imeisha1, 'idfasha1, 'macsha1, 'androididsha1, 'openudidsha1,
      'adspacetype, 'appname, 'appid, 'adplatformproviderid, 'client,
      'networkmannerid, 'ispid, 'provincename, 'cityname, 'keywords
    )
      .where("concat(imei,idfa,mac,androidid,openudid,imeimd5,idfamd5,macmd5,androididmd5,openudidmd5,imeisha1," +
        "idfasha1,macsha1,androididsha1,openudidsha1)!='' and (appname!='' or appid!='')")
      .select(array('imei, 'idfa, 'mac, 'androidid,
        'openudid, 'imeimd5, 'idfamd5, 'macmd5, 'androididmd5,
        'openudidmd5, 'imeisha1, 'idfasha1, 'macsha1,
        'androididsha1, 'openudidsha1).as("ids"),
        'adspacetype, 'appname, 'appid, 'adplatformproviderid,
        'client, 'networkmannerid, 'ispid, 'provincename, 'cityname, 'keywords)


    filteredDF.printSchema()

       filteredDF.show(20,false)

    var tagRDD: RDD[(Array[String], List[(String, Int)])] = filteredDF.rdd.map(row => {
      val idarr= row.getAs[Seq[String]](0)

      val ids: Array[String] = UserTagUtil.getUids(idarr.toArray)

      // 抽取地域属性标签（省，市）
      val areaTags = AreaTagsUtil.genTags(row)

      // 抽取用户客户端属性标签（app名称，网络类型，运营商，操作系统）
      val clientTags = ClientTagsUtil.genTagsBrod(row, dictBC.value)

      // 抽取用户页面关键字标签
      val kwTags = KeywordsTagsUtil.genTags(row, stopWdBC.value)

      // 抽取用户点击的广告栏位类型标签
      val adTags = AdTagsUtil.genTags(row)

      (ids, areaTags ++ clientTags ++ kwTags ++ adTags)

    })


    // 构造一个点集合
    val vertexRDD: RDD[(Long, List[(String, Int)])] = tagRDD.flatMap(tp => {  // tp._1 是标识列表 ，tp._2是画像标签

      val id1 = tp._1.head
      val tagList = tp._2

      tp._1.map(id => {
        if (id.equals(id1)) {
          // 第一个标识id，带上数据  作为  点元组对象
          (id1.hashCode.toLong, tagList)
        } else {
          // 其他的标识id， 不带数据，作为 点元组对象
          (id.hashCode.toLong, List.empty[(String, Int)])
        }
      })

    })

    // 构造一个边集合
    val edges: RDD[Edge[Null]] = tagRDD.flatMap(tp => {  // tp._1 是标识列表 ，tp._2是画像标签
      // 取第一个用户标识id
      val headid = tp._1.head.hashCode.toLong
      // 将第一个用户标识id和ids列表中所有的id组合成一个Edge对象
      tp._1.map(uid => Edge(headid, uid.hashCode.toLong, null))
    })

    // 构造一个图
    val graph = Graph(vertexRDD, edges)

    // 求出图中的连通子图结果 (  标识id的long值,   所属子图中的最小id的long值  )
    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // 用连通结果去join原始点集合
    val value: RDD[(VertexId, (VertexId, List[(String, Int)]))] = cc.join(vertexRDD)

    // 用最小id作为key，对画像标签数据分组聚合
    value.map({
      case (uid,(minId,tagList)) => (minId,tagList)
    }).reduceByKey((a,b)=> a ++ b)

    println(cc.count())


    spark.close()


  }
}
