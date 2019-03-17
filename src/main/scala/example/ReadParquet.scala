package example

import com.doit.hunter.dmp.utils.SessionUtil
import org.apache.spark.sql.{Dataset, Encoders, Row}

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/26 17:38
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object ReadParquet {

  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSparkSession(Map())

    val df = spark.read.parquet("g:/dmpdata/parquet")
    df.printSchema()
    df.show(50,false)


    spark.close()
  }

}

case class Person(name:String,age:Int)
case class Person2(name:String,age:Int)
object Other{
  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSparkSession(Map())
    import spark.implicits._
    import scala.collection.JavaConversions._

    val ps = List(Person("a",1))
    // val df = spark.createDataFrame(ps,classOf[Person])

    val ds = spark.createDataset(ps)

    //val dsrow2 = ds.as[Row]

    val dsrow: Dataset[Person2] = ds.map(p => Person2(p.name,p.age) )

    val df3 = dsrow.toDF()

    df3.printSchema()

    df3.show()



   /* val df2 = ds.toDF("f1","f2")
    df2.show()*/


  }
}
