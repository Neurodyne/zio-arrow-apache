package zio.serdes

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import java.util.{ List => JList }

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.FieldVector

import zio.{ Chunk }
import zio.serdes.serdes._

sealed abstract class Serdes[F[_]] {

  def serialize[A](din: F[A]): BArr
  def deserialize[A](din: BArr): F[A]

}

object Serdes {

  def apply[F[_]](implicit srd: Serdes[F]) = srd

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
  implicit val chunkArraySerdes = new Serdes[Chunk] {

    def serialize[A](din: Chunk[A]): BArr =
      scatter[Array, A](din.toArray).toByteArray

    def deserialize[A](din: BArr): Chunk[A] =
      Chunk.fromArray(gather[Array, A](din.toArray))

  }

  // Serdes for Apache Arrow
  implicit val chunkArrowSerdes = new Serdes[ChunkSchema] {

    val alloc = new RootAllocator(Integer.MAX_VALUE)

    // Write to Arrow Vector
    def writeVector[A](vectors: JList[FieldVector], len: Int, data: Chunk[A]): Unit = {

      val vec = vectors.get(0).asInstanceOf[TinyIntVector]
      vec.setValueCount(len)

      for (i <- 0 until len)
        vec.set(i, data(i).asInstanceOf[Int])

    }

    // Read from Arrow Vector
    def readVector[A](root: VectorSchemaRoot): Chunk[A] = {

      val vec = root.getFieldVectors.get(0).asInstanceOf[TinyIntVector]

      println(vec.getMinorType)

      val count = vec.getValueCount

      val out = scala.collection.mutable.ArrayBuffer[Byte]()

      for (i <- 0 until count)
        if (!vec.isNull(i))
          out += vec.get(i)

      println(out)
      Chunk.fromArray(out.toArray).asInstanceOf[Chunk[A]]
    }

    def writeStream(root: VectorSchemaRoot, numBatches: Int): BArr = {

      val out    = new ByteArrayOutputStream
      val writer = new ArrowStreamWriter(root, null, out)

      writer.start

      for (_ <- 0 until numBatches)
        writer.writeBatch

      writer.end

      val bytesWritten = writer.bytesWritten
      println(s"Bytes written: $bytesWritten")

      writer.close

      out.toByteArray

    }

    def serialize[A](din: ChunkSchema[A]): BArr = {

      // Unpack data and schema
      val (data, schema) = din

      // Write setup
      val numBatches = 1
      val length     = data.length // write vector length

      //Create a root alloc for this schema
      val root = VectorSchemaRoot.create(schema, alloc)
      root.getFieldVectors.get(0).allocateNew

      // Update metadata
      root.setRowCount(length)

      // Write to vectors
      val vectors = root.getFieldVectors
      writeVector(vectors, length, data)

      // Write to output stream
      writeStream(root, numBatches)

    }

    def deserialize[A](din: BArr): ChunkSchema[A] = {

      val stream = new ByteArrayInputStream(din)
      val reader = new ArrowStreamReader(stream, alloc)

      val root   = reader.getVectorSchemaRoot
      val schema = root.getSchema

      // Read vectors
      reader.loadNextBatch
      val out = readVector(root)

      (out, schema)

    }

  }
}
