package example

import ch.hsr.geohash.GeoHash

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/27 16:33
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object TestSplit {

  def main(args: Array[String]): Unit = {
    val line = "a,b,,u,,,"
    println(line.split(",",-1).size)


    var m1 = Map[String,Int]("a"->1,"b"->1)
    var m2 = Map[String,Int]("c"->1,"b"->2)

    m1 += "e" -> 3
    println(m1)
    println(m1 ++ m2)

    val code1 = GeoHash.withBitPrecision(39.990464d,116.481488d,60).toBase32
    val code2 = GeoHash.withBitPrecision(39.990474d,116.481478d,60).toBase32
    val code3 = GeoHash.withBitPrecision(39.990564d,116.481588d,60).toBase32
    val code4 = GeoHash.withBitPrecision(39.991464d,116.482488d,60).toBase32


    println(code1)
    println(code2)
    println(code3)
    println(code4)


  }

}
