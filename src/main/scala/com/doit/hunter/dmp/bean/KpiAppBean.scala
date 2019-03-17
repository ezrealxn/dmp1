package com.doit.hunter.dmp.bean

/**
  * Created by hunter.coder 涛哥  
  * 2019/3/1 18:35
  * 交流qq:657270652
  * Version: 1.0
  * 更多学习资料：https://blog.csdn.net/coderblack/
  * Description:
  **/
case class KpiAppBean(dt: String,
                      appname: String,
                      rawrq: Int,
                      efrq: Int,
                      adrq: Int,
                      bidrq: Int,
                      winrq: Int,
                      showrq: Int,
                      clickrq: Int,
                      consume: Double,
                      cost: Double
                     )
