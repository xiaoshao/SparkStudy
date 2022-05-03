package com.implicitd

object ImplicitDemoMain2 {

  def main(args: Array[String]): Unit = {

  }

  implicit val StringMonoid: Monoid[String] = {
    def add(x: String, y: String): String = x concat y

    def unit: String = ""
  }

  implicit val IntMonoid: Monoid[Int] = {
    def add(x: Int, y: Int) : Int = x +y
    def  unit: Int = 0
  }
}
