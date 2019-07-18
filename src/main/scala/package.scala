package object zioSerdesPkg {

  type BArr = Array[Byte]
  def eqv(x: BArr, y: BArr): Boolean = java.util.Arrays.equals(x, y)

}
