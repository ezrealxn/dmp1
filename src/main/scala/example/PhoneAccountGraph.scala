package example

import com.doit.hunter.dmp.utils.SessionUtil
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/4 11:38
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object PhoneAccountGraph {


  def main(args: Array[String]): Unit = {



    val spark = SessionUtil.getSparkSession()

    // 构造一个点的集合
    val vertexRDD: RDD[(Long, String)] = spark.sparkContext.parallelize(Seq(
      (1L, "1"),
      (2L, "2"),
      (133L, "133"),
      (6L, "6"),
      (138L, "138"),
      (7L, "7"),
      (158L, "158"),
      (5L, "5")
    ))



    // 构造一个边的集合
    val edges: RDD[Edge[String]] = spark.sparkContext.parallelize(Seq(
      Edge[String](1L,133L,""),
      Edge[String](2L,133L,""),
      Edge[String](133L,133L,""),
      Edge[String](6L,133L,""),
      Edge[String](6L,138L,""),
      Edge[String](7L,158L,""),
      Edge[String](5L,158L,"")
    ))


    // 将点集合，和边集合，构造成一张“图”
    val graph: Graph[String, String] = Graph(vertexRDD,edges)


    // connectedComponents()执行完成后得到的结果graph中，每一个点还是原来的图中的点，但是都带了一个“属性”==》能与它联通的所有点中最小的id
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices


    vertices.map(_.swap).mapValues(List(_)).reduceByKey(_++_).foreach(println)



    spark.close()

  }





}
