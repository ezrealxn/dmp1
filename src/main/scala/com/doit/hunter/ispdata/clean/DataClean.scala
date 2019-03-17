package com.doit.hunter.ispdata.clean

import com.doit.hunter.dmp.utils.SessionUtil
import org.apache.commons.lang3.StringUtils

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/3 9:39
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object DataClean {

  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSparkSession()
    val ds = spark.read.textFile("E:\\mrdata\\enhance\\input")

    import spark.implicits._
    val df = ds.map(line=>{
      val fields = StringUtils.split(line,"\t")
      (fields(6),fields(26))
    }).toDF("phone","url")




    df.printSchema()

    df.show(10,false)

    spark.close()

  }

}
