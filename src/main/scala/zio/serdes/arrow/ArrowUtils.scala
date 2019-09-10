package zio.serdes.arrow

import java.util.{ List => JList }
import java.io.ByteArrayOutputStream

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.FieldVector

import zio.{ Chunk }

import zio.serdes.serdes._

object ArrowUtils {
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
}
