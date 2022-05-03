package com.transientd

import org.apache.commons.io.output.ByteArrayOutputStream

import java.io.{ByteArrayInputStream, IOException, ObjectInputStream, ObjectOutputStream}

object Main {
  def main(args: Array[String]): Unit = {
    val name = new TransientDemo("name", 11, "saysay")

    println(name)

    val output = serialize(name)

    val desOutput = deserialize(output)

    println(desOutput)
  }

  def deserialize(in: Array[Byte]): TransientDemo = {
    var objectInputStream: ObjectInputStream = null
    val byteArrayInput = new ByteArrayInputStream(in)

    objectInputStream = new ObjectInputStream(byteArrayInput)

    val value = objectInputStream.readObject()
    value.asInstanceOf[TransientDemo]

  }

  def serialize(value: TransientDemo): Array[Byte] = {
    var objectOutputStream: ObjectOutputStream = null
    var byteArrayOutputStream: ByteArrayOutputStream = null
    try {
      byteArrayOutputStream = new ByteArrayOutputStream()
      objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
      objectOutputStream.writeObject(value)
      return byteArrayOutputStream.toByteArray
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    null
  }
}
