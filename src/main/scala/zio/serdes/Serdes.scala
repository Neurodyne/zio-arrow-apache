package zio.serdes

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import zio.serdes.serdes._

abstract class Serdes[F[_]] {

  def serialize[A](din: F[A]): BArr
  def deserialize[A](din: BArr): F[A]

}

object Serdes {

  def apply[F[_]](implicit srd: Serdes[F]) = srd

  def scatter[F[_], A](value: F[A]): ByteArrayOutputStream = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    try {
      oos.writeObject(value)
    } finally {
      oos.close()
    }
    stream
  }

  def gather[F[_], A](bytes: BArr): F[A] = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try {
      (ois.readObject()).asInstanceOf[F[A]]
    } finally {
      ois.close()
    }
  }

}
