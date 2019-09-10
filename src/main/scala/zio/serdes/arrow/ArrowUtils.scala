package zio.serdes.arrow

import java.io.ByteArrayOutputStream

import org.apache.arrow.vector.types.Types.MinorType.{ FLOAT4, TINYINT }
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.TinyIntVector

import zio.{ Chunk }

import zio.serdes.serdes._
import org.apache.arrow.vector.Float4Vector

object ArrowUtils {
  val alloc = new RootAllocator(Integer.MAX_VALUE)

  // Write to Arrow Vectors
  def writeVectors[A](root: VectorSchemaRoot, data: Chunk[A]): Unit = {
    val vectors = root.getFieldVectors
    val len     = data.length

    // Update metadata
    root.setRowCount(len)

    val size = vectors.size
    println(s"vectors size = $size")

    vectors.forEach(vec => {

      vec.setValueCount(len)
      val vtype = vec.getMinorType
      println(s"Minor type = $vtype")

      vtype match {

        case TINYINT => {
          println("Inside TINYINT vector")

          for (i <- 0 until len)
            vec.asInstanceOf[TinyIntVector].set(i, data(i).asInstanceOf[Int])
        }

        case FLOAT4 => {
          println("Inside FLOAT4 vector")

          for (i <- 0 until len)
            vec.asInstanceOf[Float4Vector].set(i, data(i).asInstanceOf[Float])
        }

        case _ =>
      }

    })
  }

  // Read from Arrow Vectors
  def readVectors[A](root: VectorSchemaRoot): Chunk[A] = {
    println("Inside reader")

    val out = scala.collection.mutable.ArrayBuffer[Float]()

    val vectors = root.getFieldVectors

    vectors.forEach(vec => {

      val vtype = vec.getMinorType
      val count = vec.getValueCount
      println(s"Minor type = $vtype")

      vtype match {

        case TINYINT =>
          for (i <- 0 until count)
            if (!vec.isNull(i))
              out += vec.asInstanceOf[TinyIntVector].get(i)

        case FLOAT4 =>
          for (i <- 0 until count)
            if (!vec.isNull(i))
              out += vec.asInstanceOf[Float4Vector].get(i)

        case _ =>
      }

    })

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
