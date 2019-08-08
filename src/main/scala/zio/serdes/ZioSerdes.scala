package zio.serdes

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ ArrowStreamReader }
import zio.{ Chunk }
// import zio.serdes._

sealed abstract class Serdes[F[_], G[_]] {

  // def serialize[A, B](din: F[A]): G[B]
  def deserialize[A, B](din: G[A]): F[B]

}

object Serdes {

  def apply[F[_], G[_]](implicit srd: Serdes[F, G]) = srd

  def scatter[F[_], A](value: F[A]): ByteArrayOutputStream = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)
    try {
      oos.writeObject(value)
    } finally {
      oos.close()
    }
    stream
  }

  def gather[F[_], A](bytes: BArr): F[A] = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try {
      (ois.readObject()).asInstanceOf[F[A]]
    } finally {
      ois.close()
    }
  }

  // Serdes for ZIO Chunk
  // implicit val chunkSerdes = new Serdes[Chunk, Chunk] {

  //   def serialize[A, Byte](din: Chunk[A]): Chunk[Byte] =
  //     Chunk.fromArray(scatter[Array, A](din.toArray).toByteArray)

  //   def deserialize[Byte, B](din: Chunk[Byte]): Chunk[B] =
  //     Chunk.fromArray(gather[Array, B](din.toArray))

  // }

  // Serdes for Apache Arrow
  implicit val arrowSerdes = new Serdes[Chunk, Chunk] {

    // type BArr = Array[Byte]

    // def serialize[A](din: ByteArrow[A]): BArr = {
    //   val bytes = Array(din.readableBytes.toByte)
    //   din.readBytes(bytes)
    //   bytes
    // }
    // def serialize

    // type delta = ArrowStreamReader

    def deserialize[Array[Byte], ArrowStreamReader](din: Chunk[BArr]): Chunk[ArrowStreamReader] =
      for {
        arr    <- din
        alloc  = new RootAllocator(Integer.MAX_VALUE)
        stream = new ByteArrayInputStream(arr)
        reader = new ArrowStreamReader(stream, alloc)

      } yield reader

  }
}
