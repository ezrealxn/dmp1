package example

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/1 14:37
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/

case class Cat(name:String)


class Dog(val tp:String){



}

object Dog{


  def apply(tp: String): Dog = new Dog(tp)

  def unapply(dog: Dog): Option[String] = Some(dog.tp)
}



object CaseDemo {

  def main(args: Array[String]): Unit = {

    // 匿名函数--》 Function类的实例（对象）.apply()
    // {case i:Int => println(i)}

    val f:PartialFunction[Int,Unit] = { case i:Int if i>3 => println(i) }


    val i: Any = 5
    i match {
      case 5 =>  println("是5")
      case x:Int => println("是整数，且是： " + x)
      case Int =>  println("是5")
    }



    val h:Any = Dog("旺财")

    h match {
      case "5" => println("这是一个5")
      case s:String => println("这是一个string " + s)
      //case String => println("这是字符串")
      case Cat => println("这是猫")
      //case d:Dog => println("这是狗狗")
      case c:Cat => println("这是一只猫： " + c.name)
      case Cat(x) => println("这是"+x+"猫")
      //case Dog(x) => println("这是一只狗狗，名字叫： " + x)
      //case Dog => println("这是狗狗")

    }


  }


}
