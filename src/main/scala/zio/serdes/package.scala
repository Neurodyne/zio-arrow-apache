package zio.serdes

// import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }
package object serdes {

  type BArr = Array[Byte]
  // type StreamReader[A] = ArrowStreamReader

  def eqv(x: BArr, y: BArr): Boolean = java.util.Arrays.equals(x, y)

}
