import zio.{ Chunk }

case class streamData[A](din: Chunk[A]) extends AnyRef with Serializable

package object zioSerdesPkg {

  type BArr = Array[Byte]

}
