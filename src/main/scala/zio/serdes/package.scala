package zio.serdes

import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }
package object serdes {

  type BArr = Array[Byte]

  type ArrStreamWriter[A] = ArrowStreamWriter
  type ArrStreamReader[A] = ArrowStreamReader

  def eqv(x: BArr, y: BArr): Boolean = java.util.Arrays.equals(x, y)

}
