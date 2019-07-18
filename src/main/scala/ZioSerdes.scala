package zioSerdes

import org.apache.commons.lang3.SerializationUtils
import zio.{ Chunk }

case class StreamData[A](din: Chunk[A]) extends AnyRef with Serializable

sealed abstract class Serdes[F[_], G[_]] {

  def serialize[A](din: F[A]): G[Byte]
  def deserialize[A](din: G[Byte]): F[A]

}

object Serdes {

  implicit val chunkSerdes = new Serdes[StreamData, Chunk] {

    def serialize[A](din: StreamData[A]): Chunk[Byte] =
      Chunk.fromArray(SerializationUtils.serialize(din))

    def deserialize[A](din: Chunk[Byte]): StreamData[A] =
      SerializationUtils.deserialize(din.toArray)

  }
}
