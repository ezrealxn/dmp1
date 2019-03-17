package example

import com.doit.hunter.dmp.utils.SessionUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/4 15:03
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object IncomeGraph {

  def main(args: Array[String]): Unit = {

    val spark = SessionUtil.getSparkSession()


    // 读取原始数据，并按逗号切成数组，并缓存
    val rdd = spark.sparkContext.textFile("g:/dmpdata/dehua/").map(_.split(",",-1)).cache()

    // 构建sparkgraphx点击和
    val vertexRDD: RDD[(Long, List[(String, Int)])] = rdd.flatMap(arr=>{

      // 准备一个list来缓存点元组对象
      var list = List[(Long,List[(String,Int)])]()

      // 过滤掉 为空的标识 字段
      val arr2 = for(elem <- arr if StringUtils.isNotBlank(elem)) yield elem

      // 从数组的第2个开始， 拼接一个点元组对象（ 标识字段值的hashcode tolong，List（标识字段自身，钱数全为0）
      for(i <- 1 until arr2.size -1 ){
        list :+= (arr2(i).hashCode.toLong,List((arr2(i),0)))
      }

      // 为数组的第一个标识字段，构造一个 点元组对象：  （ 标识字段值的hashcode tolong，List（标识字段自身，钱数）
      list :+= (arr2(0).hashCode.toLong,List((arr2(0),arr2(arr2.size-1).toInt)))

      list
    })


    //  构建边集合
    val edges: RDD[Edge[Null]] = rdd.flatMap(arr=>{

      // 过滤掉空标识字段
      val arr2 = for(elem <- arr if StringUtils.isNotBlank(elem)) yield elem

      // 取标识字段数组的第一个标识
      val id0 = arr2(0)


      var list = List[Edge[Null]]()

      // 从标识字段数组的第2个开始，拼接边对象 Edge(起始点（永远用第0个），结束点（当前遍历到的那一个）,null)
      for(i <- 1 until arr2.length-1 ){
        list :+=  Edge(id0.hashCode.toLong,arr2(i).hashCode.toLong,null)
      }
      list

    })


    // 利用 点集合   +   边集合  ==》 构建一个“图”
    val graph = Graph(vertexRDD,edges)

    // 调用 图计算api的 连通图算法，得到连通子图结果(  （点，所属子图的最小点id） )

    val cc = graph.connectedComponents().vertices

    // 用连通子图结果 的点  去  join   原点集合，得到点的属性数据，以便做聚合运算
    cc.join(vertexRDD).map({

      // TODO
      (k,v)
    }).reduceByKey(_+_)

    spark.close()



  }

}
