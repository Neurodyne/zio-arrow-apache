package zio.serdes

import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }
import org.apache.arrow.vector.types.pojo.{ Schema }

import zio.{ Chunk }

package object serdes {

  type BArr = Array[Byte]

  type ArrStreamWriter[A] = ArrowStreamWriter
  type ArrStreamReader[A] = ArrowStreamReader

  type ChunkSchema[A] = (Chunk[A], Schema)

  def eqv(x: BArr, y: BArr): Boolean = java.util.Arrays.equals(x, y)

}
