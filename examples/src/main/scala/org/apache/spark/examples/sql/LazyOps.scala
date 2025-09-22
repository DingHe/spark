package org.apache.spark.examples.sql

object LazyOps {

  def init(): String = {
    println("call init()")
    return ""
  }

  def main(args: Array[String]) {
    lazy val property = init();//使用lazy修饰
    println("after init()")
    println(property)
    println(property)
  }

}