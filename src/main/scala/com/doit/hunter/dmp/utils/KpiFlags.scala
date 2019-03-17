package com.doit.hunter.dmp.utils

import org.apache.spark.sql.Row

/**
  * Created by hunter.coder 涛哥  
  * 2019/2/27 18:12
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
object KpiFlags {


  // 将一行数据经过条件判断后，返回各种请求类型的0/1标记
  def caculateKpi(row:Row): List[Double] ={

    val rqmode = row.getAs[Int]("requestmode")
    val pnode = row.getAs[Int]("processnode")
    val isef = row.getAs[Int]("iseffective")
    val isbil = row.getAs[Int]("isbilling")
    val isbd = row.getAs[Int]("isbid")
    val iswin = row.getAs[Int]("iswin")
    val adoid = row.getAs[Int]("adorderid")
    val price = row.getAs[Double]("winprice")
    val payment = row.getAs[Double]("adpayment")


    var Array(rawrq,efrq,adrq,bidrq,winrq,showrq,clickrq,consume,cost) = Array(0,0,0,0,0,0,0,0.0,0.0)

    if(rqmode==1 && pnode>=1) rawrq = 1
    if(rqmode==1 && pnode>=2) efrq = 1
    if(rqmode==1 && pnode==3) adrq = 1
    if(isef==1 && isbil==1 && isbd ==1 && adoid!=0) bidrq = 1
    if(isef==1 && isbil==1 && iswin ==1) winrq = 1

    if(rqmode==2 && isef==1) showrq = 1
    if(rqmode==3 && isef==1) clickrq = 1

    if(isef==1 && isbil==1 && iswin ==1){
      consume = price/1000d
      cost = payment/1000d
    }

    List(rawrq,efrq,adrq,bidrq,winrq,showrq,clickrq,consume,cost)

  }

}
