package com.doit.hunter.dmp.clean

import java.net.URI

import com.doit.hunter.dmp.utils.{JudgeIfDirty, SessionUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * Created by hunter.coder 涛哥
  * 2019/2/26 16:45
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:  过滤掉脏数据（字段数量少于85过滤掉，转不成需求说明文档指定类型的）
  **/
object Bzip2Parquet {


  def main(args: Array[String]): Unit = {

    val spark = SessionUtil.getSparkSession(Map())
    import spark.implicits._


    // 读取原始压缩文件
    val rawDs: Dataset[String] = spark.read.textFile("G:\\dmpdata\\raw")

    println("raw data : " + rawDs.count())

    // 过滤掉字段数少于85的数据
    val filterSizeDs: Dataset[Array[String]] = rawDs.map(_.split(",", -1)).filter(arr => {
      arr.length >= 85
    })

    // 过滤掉无法转整数、double等类型的脏数据
    val cleanDs: Dataset[Array[String]] = filterSizeDs.filter(JudgeIfDirty.isDirty(_))

    println("cleanDs data : " + cleanDs.count())

    // cleanDs是一个dataset，不能通过map方法直接将其中的arr转成row，需要设置一个自定义的 Encoder
    // 如果通过rdd的map方法，来将arr转成Row就没有问题
    val rowRdd: RDD[Row] = cleanDs.rdd.map(arr => Row(
      arr(0),
      arr(1).toInt,
      arr(2).toInt,
      arr(3).toInt,
      arr(4).toInt,
      arr(5),
      arr(6),
      arr(7).toInt,
      arr(8).toInt,
      arr(9).toDouble,
      arr(10).toDouble,
      arr(11),
      arr(12),
      arr(13),
      arr(14),
      arr(15),
      arr(16),
      arr(17).toInt,
      arr(18),
      arr(19),
      arr(20).toInt,
      arr(21).toInt,
      arr(22),
      arr(23),
      arr(24),
      arr(25),
      arr(26).toInt,
      arr(27),
      arr(28).toInt,
      arr(29),
      arr(30).toInt,
      arr(31).toInt,
      arr(32).toInt,
      arr(33),
      arr(34).toInt,
      arr(35).toInt,
      arr(36).toInt,
      arr(37),
      arr(38).toInt,
      arr(39).toInt,
      arr(40).toDouble,
      arr(41).toDouble,
      arr(42).toInt,
      arr(43),
      arr(44).toDouble,
      arr(45).toDouble,
      arr(46),
      arr(47),
      arr(48),
      arr(49),
      arr(50),
      arr(51),
      arr(52),
      arr(53),
      arr(54),
      arr(55),
      arr(56),
      arr(57).toInt,
      arr(58).toDouble,
      arr(59).toInt,
      arr(60).toInt,
      arr(61),
      arr(62),
      arr(63),
      arr(64),
      arr(65),
      arr(66),
      arr(67),
      arr(68),
      arr(69),
      arr(70),
      arr(71),
      arr(72),
      arr(73).toInt,
      arr(74).toDouble,
      arr(75).toDouble,
      arr(76).toDouble,
      arr(77).toDouble,
      arr(78).toDouble,
      arr(79),
      arr(80),
      arr(81),
      arr(82),
      arr(83),
      arr(84).toInt
    ))


    // 将过滤好的数据，转成Row Dataset


    // 构造一个schema
    val schema = new StructType(Array(
      StructField("sessionid", StringType), // 会话标识
      StructField("advertisersid", IntegerType), // 广告主id
      StructField("adorderid", IntegerType), // 广告id
      StructField("adcreativeid", IntegerType), // 广告创意id ( >= 200000 ", dsp)
      StructField("adplatformproviderid", IntegerType), // 广告平台商id (>= 100000", rtb)
      StructField("sdkversion", StringType), // sdk 版本号
      StructField("adplatformkey", StringType), // 平台商key
      StructField("putinmodeltype", IntegerType), // 针对广告主的投放模式                                                             //1：展示量投放2：点击
      StructField("requestmode", IntegerType), // 数据请求方式（1",请求、2",展示、3",点击）
      StructField("adprice", DoubleType), // 广告价格
      StructField("adppprice", DoubleType), // 平台商价格
      StructField("requestdate", StringType), // 请求时间                                                             //格式为：yyyy-m-dd hh",mm",ss
      StructField("ip", StringType), // 设备用户的真实ip 地址
      StructField("appid", StringType), // 应用id
      StructField("appname", StringType), // 应用名称
      StructField("uuid", StringType), // 设备唯一标识
      StructField("device", StringType), // 设备型号，如htc、iphone
      StructField("client", IntegerType), // 操作系统（1：android 2：ios 3：wp）
      StructField("osversion", StringType), // 设备操作系统版本
      StructField("density", StringType), // 设备屏幕的密度
      StructField("pw", IntegerType), // 设备屏幕宽度
      StructField("ph", IntegerType), // 设备屏幕高度
      StructField("lng", StringType), // 设备所在经度
      StructField("lat", StringType), // 设备所在纬度
      StructField("provincename", StringType), // 设备所在省份名称
      StructField("cityname", StringType), // 设备所在城市名称
      StructField("ispid", IntegerType), // 运营商id
      StructField("ispname", StringType), // 运营商名称
      StructField("networkmannerid", IntegerType), // 联网方式id
      StructField("networkmannername", StringType), //联网方式名称
      StructField("iseffective", IntegerType), // 有效标识（有效指可以正常计费的）(0：无效1：
      StructField("isbilling", IntegerType), // 是否收费（0：未收费1：已收费）
      StructField("adspacetype", IntegerType), // 广告位类型（1：banner 2：插屏3：全屏）
      StructField("adspacetypename", StringType), // 广告位类型名称（banner、插屏、全屏）
      StructField("devicetype", IntegerType), // 设备类型（1：手机2：平板）
      StructField("processnode", IntegerType), // 流程节点（1：请求量kpi 2：有效请求3：广告请
      StructField("apptype", IntegerType), // 应用类型id
      StructField("district", StringType), // 设备所在县名称
      StructField("paymode", IntegerType), // 针对平台商的支付模式，1：展示量投放(CPM) 2：点击
      StructField("isbid", IntegerType), // 是否rtb
      StructField("bidprice", DoubleType), // rtb 竞价价格
      StructField("winprice", DoubleType), // rtb 竞价成功价格
      StructField("iswin", IntegerType), // 是否竞价成功
      StructField("cur", StringType), // values",usd|rmb 等
      StructField("rate", DoubleType), // 汇率
      StructField("cnywinprice", DoubleType), // rtb 竞价成功转换成人民币的价格
      StructField("imei", StringType), // imei
      StructField("mac", StringType), // mac
      StructField("idfa", StringType), // idfa
      StructField("openudid", StringType), // openudid
      StructField("androidid", StringType), // androidid
      StructField("rtbprovince", StringType), // rtb 省
      StructField("rtbcity", StringType), // rtb 市
      StructField("rtbdistrict", StringType), // rtb 区
      StructField("rtbstreet", StringType), // rtb 街道
      StructField("storeurl", StringType), // app 的市场下载地址
      StructField("realip", StringType), // 真实ip
      StructField("isqualityapp", IntegerType), // 优选标识
      StructField("bidfloor", DoubleType), // 底价
      StructField("aw", IntegerType), // 广告位的宽
      StructField("ah", IntegerType), // 广告位的高
      StructField("imeimd5", StringType), // imei_md5
      StructField("macmd5", StringType), // mac_md5
      StructField("idfamd5", StringType), // idfa_md5
      StructField("openudidmd5", StringType), // openudid_md5
      StructField("androididmd5", StringType), // androidid_md5
      StructField("imeisha1", StringType), // imei_sha1
      StructField("macsha1", StringType), // mac_sha1
      StructField("idfasha1", StringType), // idfa_sha1
      StructField("openudidsha1", StringType), // openudid_sha1
      StructField("androididsha1", StringType), // androidid_sha1
      StructField("uuidunknow", StringType), // uuid_unknow tanx 密文
      StructField("userid", StringType), // 平台用户id
      StructField("iptype", IntegerType), // 表示ip 类型
      StructField("initbidprice", DoubleType), // 初始出价
      StructField("adpayment", DoubleType), // 转换后的广告消费
      StructField("agentrate", DoubleType), // 代理商利润率
      StructField("lrate", DoubleType), // 代理利润率
      StructField("adxrate", DoubleType), // 媒介利润率
      StructField("title", StringType), // 标题
      StructField("keywords", StringType), // 关键字
      StructField("tagid", StringType), // 广告位标识(当视频流量时值为视频ID 号)
      StructField("callbackdate", StringType), // 回调时间格式为",YYYY/mm/dd hh",mm",ss
      StructField("channelid", StringType), // 频道ID
      StructField("mediatype", IntegerType) // 媒体类型：1 长尾媒体2 视频媒体3 独立媒体默认",1
    )

    )

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path("g:/dmpdata/parquet")
    if(fs.exists((path))){
      fs.delete(path,true)
      println("deleted output path ......")
    }

    // 把rdd转成dataframe
    val frame: DataFrame =  spark.createDataFrame(rowRdd,schema)
    frame.write.parquet("g:/dmpdata/parquet/")

    frame.show(20,false)

    spark.close()

  }


}
