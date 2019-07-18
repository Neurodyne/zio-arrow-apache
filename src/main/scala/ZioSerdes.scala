package zioSerdes

import org.apache.commons.lang3.SerializationUtils

import zio.{ Chunk, Task, ZIO }
import zioSerdesPkg._
// import simulacrum._

// @typeclass abstract class ZioSerdes[F[_]] {
// @typeclass trait ZioSerdes[F[_], G[_]] {
/* @typeclass trait ZioSerdes[F[_]] {

  @op(">>>") def serialize[A, B](din: F[A]): F[B]

  // @op("<<<") def deserialize[A, B](din: F[B]): F[A]
} */

case class streamData[A](din: Chunk[A]) extends AnyRef with Serializable

sealed abstract class Serdes[F[_]] {

  // def serialize[A, B](din: F[A]): G[B]

  def serialize[A](din: F[A]): Chunk[Byte]

  // @op("<<<") def deserialize[A, B](din: F[B]): F[A]
}

// val inst = new ZioSerdes

object Serdes {

  implicit val chunkSerdes = new Serdes[streamData] {

    // def serialize[A, B] (din:Chunk[A]):Task[B] = ZIO.effect(SerializationUtils.serialize(din))
    def serialize[A](din: streamData[A]): Chunk[Byte] =
      Chunk.fromArray(SerializationUtils.serialize(din))
    /* din.map ( r =>
        // ZIO.effect(SerializationUtils.serialize(r))


      ) */
    // def deserialize[A,B] (din:Chunk[B]):Chunk[A] = ZIO.effect(SerializationUtils.deserialize(din))

  }

}
