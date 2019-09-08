package zio.serdes

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import zio.{ Chunk }

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }

import zio.serdes.serdes._
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
import java.util.Arrays.asList
import java.util.Collections

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
  implicit val chunkArrowSerdes = new Serdes[Chunk, ArrStreamReader] {

    val testSchema = new Schema(
      asList(new Field("testField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()))
    )

    val alloc = new RootAllocator(Integer.MAX_VALUE)
    val root  = VectorSchemaRoot.create(testSchema, alloc)

    def serialize[A](din: Chunk[A]): BArr = {

      val out    = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, out)
      writer.close

      out.toByteArray

    }

    def deserialize[A](din: BArr): ArrStreamReader[A] = {

      val stream = new ByteArrayInputStream(din)
      val reader = new ArrowStreamReader(stream, alloc)

      reader
    }

  }

}
