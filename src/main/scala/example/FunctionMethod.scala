package example

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/27 11:37
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
class FunctionMethod {


  def sayHello(name:String,age:Int):Unit = {

    println(name + " is  " + age +" yeas old!")

  }
    val a:String = "hello"

    val x: String => Unit = (s:String)=>{println("hello")}

    val y:(String,Int)=>Unit =  sayHello _
}

object FunctionMethod{

  def main(args: Array[String]): Unit = {

    val x = 5

    x match {
      case 5 => println("确实是5")
      case i:Int => println(i)
    }


    val arr = Array(1,2,3,45)

    arr match {
      case Array(1,2,3,45) => println("确实是123，45这个数组")
      case x:Array[Int] => println("确实是个整数数组")
      case Array(x,y,z,p) => println("这是一个4元素数组: ",x,y,z,p)
      case Array(_) => println("是一个只有一个元素的数组")
      case _ => println("不是一个元素的数组")
    }



    def callbynametest1(t: ()=> Long)= {


    }

    // call by name
    def callbynametest(t: => Long)= {


    }




  }


}