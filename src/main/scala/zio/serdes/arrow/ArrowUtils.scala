package zio.serdes.arrow

import java.io.ByteArrayOutputStream

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.TinyIntVector

import zio.{ Chunk }

import zio.serdes.serdes._

object ArrowUtils {
  val alloc = new RootAllocator(Integer.MAX_VALUE)

  // Write to Arrow Vector
  def writeVectors[A](root: VectorSchemaRoot, data: Chunk[A]): Unit = {
    val vectors = root.getFieldVectors
    val len     = data.length

    // Update metadata
    root.setRowCount(len)

    val size = vectors.size
    println(s"vectors size = $size")

    for (i <- 0 until size) {

      val vec = vectors.get(i).asInstanceOf[TinyIntVector]
      vec.setValueCount(len)

      vec.getMinorType match {
        case _ => {
          for (i <- 0 until len)
            vec.set(i, data(i).asInstanceOf[Int])

        }

      }

    }

  }

  // Read from Arrow Vector
  def readVectors[A](root: VectorSchemaRoot): Chunk[A] = {

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
