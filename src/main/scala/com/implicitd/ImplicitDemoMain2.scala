package com.implicitd

object ImplicitDemoMain2 {

  def main(args: Array[String]): Unit = {
    println(sum(List(1, 2, 3)))

    println(sum( List("A", "B", "C")))
  }

  implicit val StringMonoid: Monoid[String] = new Monoid[String] {
    def add(x: String, y: String): String = x concat y

    def unit: String = ""
  }

  implicit val IntMonoid: Monoid[Int] = new Monoid[Int] {
    def add(x: Int, y: Int): Int = x + y

    def unit: Int = 0
  }

  def sum[A](xs: List[A])(implicit m: Monoid[A]): A = {
    if (xs.isEmpty) {
      m.unit
    } else {
      m.add(xs.head, sum(xs.tail))
    }
  }
}
