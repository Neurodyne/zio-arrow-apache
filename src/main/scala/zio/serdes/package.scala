package zio

package object serdes {
  import io.netty.buffer.ArrowBuf

  type BArr         = Array[Byte]
  type ByteArrow[A] = ArrowBuf

  def eqv(x: BArr, y: BArr): Boolean = java.util.Arrays.equals(x, y)

}
