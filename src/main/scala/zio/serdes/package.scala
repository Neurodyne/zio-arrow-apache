package zio

package object serdes {
  import org.apache.arrow.vector.ipc.ArrowStreamReader
  // import io.netty.buffer.ArrowBuf

  type BArr = Array[Byte]
  // type ByteArrow[A] = ArrowBuf
  type ArrowReader[A] = ArrowStreamReader

  def eqv(x: BArr, y: BArr): Boolean = java.util.Arrays.equals(x, y)

}
