package zio.serdes

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import zio.{ Chunk }

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }

import zio.serdes.serdes._
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.TinyIntVector

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
  implicit val chunkArrowSerdes = new Serdes[ChunkSchema, ChunkSchema] {

    val alloc = new RootAllocator(Integer.MAX_VALUE)

    def serialize[A](din: ChunkSchema[A]): BArr = {

      // Unpack data and schema
      val (data, schema) = din

      //Create a root alloc for this schema
      val root = VectorSchemaRoot.create(schema, alloc)

      // Write batchs
      val numBatches = 1

      root.getFieldVectors.get(0).allocateNew
      val vec = root.getFieldVectors.get(0).asInstanceOf[TinyIntVector]

      vec.set(1, 1)
      vec.setValueCount(1)
      root.setRowCount(1)

      // Write to output
      val out    = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, out)

      writer.start

      for (i <- 0 until numBatches)
        writer.writeBatch

      writer.end

      val bytesWritten = writer.bytesWritten
      println(s"Bytes written: $bytesWritten")

      writer.close

      out.toByteArray

    }

    def deserialize[A](din: BArr): ChunkSchema[A] = {

      val stream = new ByteArrayInputStream(din)
      val reader = new ArrowStreamReader(stream, alloc)

      val schema = reader.getVectorSchemaRoot.getSchema

      val out = Chunk(1, 2, 3).asInstanceOf[Chunk[A]]
      (out, schema)
    }

  }

}
