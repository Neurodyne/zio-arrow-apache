package zio.serdes

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import zio.{ Chunk }

case class StreamData[A](din: Chunk[A]) extends AnyRef with Serializable

sealed abstract class Serdes[F[_], G[_]] {

  def serialize[A](din: F[A]): G[Byte]
  def deserialize[A](din: G[Byte]): F[A]

}

object Serdes {

  def apply[F[_], G[_]](implicit srd: Serdes[F,G]) = srd

  def scatter[F[_], A](value: F[A]): BArr = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    try {
      oos.writeObject(value)
    } finally {
      oos.close()
    }
    stream.toByteArray
  }

  def gather[F[_], A](bytes: BArr): F[A] = {
    val ois   = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try {
      (ois.readObject()).asInstanceOf[F[A]]
    } finally {
      ois.close()
    }
  }

  implicit val chunkSerdes = new Serdes[Chunk, Chunk] {

    def serialize[A](din: Chunk[A]): Chunk[Byte] =
      Chunk.fromArray(scatter[Array, A](din.toArray))

    def deserialize[A](din: Chunk[Byte]): Chunk[A] =
      Chunk.fromArray(gather[Array, A](din.toArray))

  }
}
