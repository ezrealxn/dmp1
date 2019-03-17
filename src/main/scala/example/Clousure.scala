package example

import com.doit.hunter.dmp.utils.SessionUtil

import scala.collection.mutable

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/1 9:06
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object Clousure {


  def main(args: Array[String]): Unit = {

    val hashMap = new mutable.HashMap[String,Int]()

    val spark = SessionUtil.getSparkSession()
    import spark.implicits._


    val rdd = spark.read.parquet("g:/dmpdata/parquet").rdd

    val rdd2 = rdd.map(row => {
      row.getAs[Int]("requestmode") + 1
      //hashMap.put(i.toString, i)
    })




    rdd2.count()


    for (elem <- hashMap) {

      println(elem)
    }

    spark.close()


  }

}
