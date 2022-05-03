package com.implicitd

abstract class  Monoid [A]{
  def add(x :A , y : A) : A

  def unit : A
}


