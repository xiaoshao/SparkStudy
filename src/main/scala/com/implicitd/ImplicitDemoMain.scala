package com.implicitd

object ImplicitDemoMain {

  def showStr(implicit str: String) = println(str)

  def main(args: Array[String]): Unit = {
    fun1()
  }

  def fun1(): Unit ={
    implicit val str = "hello world"

    showStr
  }



}
