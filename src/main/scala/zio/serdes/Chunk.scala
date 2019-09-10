package zio.serdes

import zio.Chunk

import zio.serdes.serdes._
import zio.serdes.Serdes._

object ChunkSerdes extends Serdes[Chunk] {

  def serialize[A](din: Chunk[A]): BArr =
    scatter[Array, A](din.toArray).toByteArray

  def deserialize[A](din: BArr): Chunk[A] =
    Chunk.fromArray(gather[Array, A](din.toArray))

}
