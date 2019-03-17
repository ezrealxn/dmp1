package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.{Success, Try}

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/26 11:36
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
case class P(val b:String)

object Movie2Parquet {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local[*]").appName("demo").getOrCreate()


    val df = spark.read.json("E:\\mrdata\\movie\\input")


    val df2 = df.drop("raete", "ratee", "tixeStamp", "xd")


    val isNumeric: String => Boolean = (s: String) => {
      val ti = Try(s.toInt)
      ti match {
        case Success(i) => true
        case _ => false
      }
    }

    //  如果评分字段的值不是数字，则过滤掉，使用rdd方式
    // val rddFlt: RDD[Row] = df2.rdd.filter(row => { isNumeric(row.getAs[String]("rate"))})
    df2.rdd.filter(row => { isNumeric(row.getAs[String]("rate"))

    })

    /*val df_filtered = df2.rdd.filter(row => {
      try {
        val i = row.getAs[String]("rate")
      } catch {
        case e: Exception => false
      }
      true
    })


    val schema = new StructType(Array(
      new StructField("movie",StringType),
      new StructField("rate",StringType),
      new StructField("timeStamp",StringType),
      new StructField("uid",StringType)
    ))

    val dfResult: DataFrame = spark.createDataFrame(df_filtered,schema)*/


    // 过滤评分脏数据，使用sql方式
    // 注册了一个sql的udf
    spark.udf.register("isnumeric", isNumeric)
    val dfFlt2 = df2.filter("isnumeric(rate)")

    println(df2.count())
    println(dfFlt2.count())


    dfFlt2.write.parquet("g:/json2parquet/")

    dfFlt2.coalesce(1).write.json("g:/clean")

    spark.close()

    // RDD [caseclass] => .toDF => dataframe[Row]
    // RDD[ROW] + schema => createDataFrame => dataframe
    // RDD[p <: Product] => .toDF =>dataframe


  }


}
