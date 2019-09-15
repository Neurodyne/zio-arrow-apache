package zio.serdes

import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }
import org.apache.arrow.vector.types.pojo.{ Schema }

import zio.{ Chunk }

final case class ArrConfig(
  vectors: Int,
  batches: Int,
  schema: Schema
)

package object Types {

  type BArr = Array[Byte]

  type ArrStreamWriter[A] = ArrowStreamWriter
  type ArrStreamReader[A] = ArrowStreamReader

  type ChunkArrow[A] = (Chunk[A], ArrConfig)

  type ChunkSchema[A]     = (Chunk[A], Schema)
  type Chunk2Schema[A, B] = (Chunk[A], Chunk[B], Schema)
  // def eqv(x: BArr, y: BArr): Boolean = java.util.Arrays.equals(x, y)

}
