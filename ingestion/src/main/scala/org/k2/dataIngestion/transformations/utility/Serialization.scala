package org.k2.dataIngestion.transformations.utility

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

/**
  * Utility used to serialize or deserialize complex objects
  */
object Serialization extends App {

  /**
    * Converts an object to Array[Byte]
    * @param value  value to be converted
    * @return Array[Byte] from value
    */
  def serialize(value: Any)= {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  /**
    * Converts an Array[Byte] to the corresponding object
    * @param bytes  Array[Byte] to be converted
    * @return the object obtained
    */
  def deserialize(bytes: Array[Byte])= {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value
  }
}