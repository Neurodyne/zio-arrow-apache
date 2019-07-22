package zio

package object serdes {

  type BArr = Array[Byte]
  def eqv(x: BArr, y: BArr): Boolean = java.util.Arrays.equals(x, y)

}
