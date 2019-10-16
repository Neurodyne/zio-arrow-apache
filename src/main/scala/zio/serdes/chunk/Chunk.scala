// package zio.serdes

// import zio.Chunk

// import zio.serdes.Types._
// import zio.serdes.Serdes._

// import scala.reflect.ClassTag

// object ChunkSerdes extends Serdes[Chunk] {

//   def serialize[A:ClassTag](din: Chunk[A]): BArr =
//     scatter[Array, A](din.toArray).toByteArray

//   def deserialize[A](din: BArr): Chunk[A] =
//     Chunk.fromArray(gather[Array, A](din.toArray))

// }
