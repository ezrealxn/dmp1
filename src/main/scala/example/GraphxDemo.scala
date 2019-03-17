package example

import com.doit.hunter.dmp.utils.SessionUtil
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/1 17:04
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object GraphxDemo {

  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSparkSession()
    val vetexs:RDD[(Long,String)]= spark.sparkContext.makeRDD(Seq(
      (1L, "zs1"),
      (2L, "zs2"),
      (3L, "zs3"),
      (4L, "zs4"),
      (5L, "zs5"),
      (6L, "zs6"),
      (7L, "zs7"),
      (8L, "zs8")
    ))

    val edges: RDD[Edge[String]] = spark.sparkContext.parallelize(Seq(
      Edge(1L,3L,"ok"),
      Edge(1L,5L,"ok"),
      Edge(5L,6L,"ok"),
      Edge(2L,4L,"ok"),
      Edge(7L,8L,"ok"),
      Edge(7L,2L,"ok")
    ))

    val graph = Graph(vetexs,edges)

    val conn: Graph[VertexId, String] = graph.connectedComponents()

    val vertices: VertexRDD[VertexId] = conn.vertices
    vertices.map(vd=>(vd._2,List(vd._1))).reduceByKey((lst1,lst2)=> lst1 ++ lst2 ).foreach(println)


    spark.close()




  }

}