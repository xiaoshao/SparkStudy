package com.implicitd

object ImplicitDemoMain {

  def showStr(implicit str: String) = println(str)

  def main(args: Array[String]): Unit = {
    fun1()

    5.times(println("Hi"))
  }

  def fun1(): Unit = {
    implicit val str = "hello world"

    showStr
  }

  implicit class Transfer(x: Int) {
    def times[A](f: => A): Unit = {
      def loop(current: Int): Unit = {
        if (current > 0) {
          f
          loop(current - 1)
        }
      }

      loop(x)
    }
  }

}
