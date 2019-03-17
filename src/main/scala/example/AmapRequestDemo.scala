package example

import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpClient, HttpMethod}

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/2 17:50
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object AmapRequestDemo {


  // 高德web服务逆地理位置编码，服务url==> https://restapi.amap.com/v3/geocode/regeo?parameters
  // 参数：  key=e384b6b8bc2f8e9e9e92a9cf969da45c   location=lng,lat

  /**
    * <!-- web服务请求工具包：httpclient -->
    * <dependency>
    * <groupId>org.apache.httpcomponents</groupId>
    * <artifactId>httpclient</artifactId>
    * <version>4.5.7</version>
    * </dependency>
    *
    * @param args
    */


  def main(args: Array[String]): Unit = {

    val client = new HttpClient()

    val method = new GetMethod("https://restapi.amap.com/v3/geocode/regeo?key=e384b6b8bc2f8e9e9e92a9cf969da45c&location=116.480656,39.989677")
    //val method = new GetMethod("http://www.baidu.com")

    val http_status = client.executeMethod(method)
    println(http_status)

    println("----------------------------------------")
    println(method.getResponseBodyAsString())


    method.releaseConnection()



  }

}
