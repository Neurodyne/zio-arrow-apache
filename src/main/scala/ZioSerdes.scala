package zioSerdes

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import zio.{ Chunk }
import zioSerdesPkg._

case class StreamData[A](din: Chunk[A]) extends AnyRef with Serializable

sealed abstract class Serdes[F[_], G[_]] {

  def serialize[A](din: F[A]): G[Byte]
  def deserialize[A](din: G[Byte]): F[A]

}

object Serdes {

  def scatter[A](value: A): BArr = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  def gather[A](bytes: BArr): A = {
    val ois   = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value.asInstanceOf[A]
  }

  implicit val chunkSerdes = new Serdes[StreamData, Chunk] {

    def serialize[A](din: StreamData[A]): Chunk[Byte] =
      Chunk.fromArray(scatter(din))

    def deserialize[A](din: Chunk[Byte]): StreamData[A] = {
      val bytes: BArr = din.toArray
      StreamData(Chunk(gather(bytes)))

    }

  }
}
