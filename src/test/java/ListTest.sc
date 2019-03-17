import scala.collection.mutable.ListBuffer

val lst1 = List(("APP_aqy",1),("DN00010001",1),("ZP_hb",1))
val lst2 = List(("APP_ddz",1),("DN00010001",1),("ZP_hb",1))


val tmp3 = (lst1++lst2).groupBy(_._1).mapValues(_.size)





val tagList = new ListBuffer[(String,Int)]

tagList += (("a",1))
tagList += (("b",2))

println(tagList)