package com.transientd

class TransientDemo(name: String, interval: Int, @transient say : String) extends Serializable {

  override def toString = s"name is $name interval is $interval say is $say"
}
