package zio.serdes

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import zio.{ Chunk }

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }

import zio.serdes.serdes._

sealed abstract class Serdes[F[_], G[_]] {

  def serialize[A](din: F[A]): BArr
  def deserialize[A](din: BArr): G[A]

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
  implicit val chunkArraySerdes = new Serdes[Chunk, Chunk] {

    def serialize[A](din: Chunk[A]): BArr =
      scatter[Array, A](din.toArray).toByteArray

    def deserialize[A](din: BArr): Chunk[A] =
      Chunk.fromArray(gather[Array, A](din.toArray))

  }

  // Serdes for Apache Arrow
  implicit val chunkArrowSerdes = new Serdes[ArrStreamWriter, ArrStreamReader] {

    def serialize[A](din: ArrStreamWriter[A]): BArr = {
      val tmp: BArr = Array(1, 2, 3)
      tmp
    }

    def deserialize[A](din: BArr): ArrStreamReader[A] = {
      val alloc  = new RootAllocator(Integer.MAX_VALUE)
      val stream = new ByteArrayInputStream(din)
      val reader = new ArrowStreamReader(stream, alloc)

      reader
    }

  }

}
