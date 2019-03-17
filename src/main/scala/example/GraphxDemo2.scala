package example

import com.doit.hunter.dmp.utils.SessionUtil
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/3 19:38
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object GraphxDemo2 {

  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSparkSession()

    import spark.implicits._

    val rawds = spark.read.textFile("E:\\mrdata\\friends\\input")

    val vertex: RDD[(Long, String)] = rawds.rdd.flatMap(line=>{
      val splits = line.split(":")

      val u = splits(0)
      val frs = splits(1).split(",")
      val all = frs ++ u

      all.map(f=>(u.hashCode.toLong,u))

    })

    val edges: RDD[Edge[String]] = rawds.rdd.flatMap(line=>{
      val splits = line.split(":")

      val u = splits(0)
      splits.map(f=>Edge(u.hashCode.toLong,f.hashCode.toLong,""))
    })

    val graph = Graph(vertex,edges)

    val cc = graph.
    ().vertices

    cc.join(vertex).map{
      case (u,(minId,name)) =>(minId,Set(name))
    }.reduceByKey((a,b)=>{
      a ++ b
    }).foreach(println)

    spark.close()
  }

}
