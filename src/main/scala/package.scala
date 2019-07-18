import zio.{ Chunk }

package object zioSerdesPkg {

  type BArr      = Array[Byte]
  type SChunk[A] = Chunk[A] with Serializable

}
