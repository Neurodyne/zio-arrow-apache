package zio.serdes.arrow

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.Types.MinorType.{ FLOAT4, FLOAT8, TINYINT, VARCHAR }
import org.apache.arrow.vector.{ Float4Vector, Float8Vector }
import org.apache.arrow.vector.{ FieldVector, TinyIntVector, VarCharVector, VectorSchemaRoot }

import zio.Chunk
import zio.serdes.Types._

object ArrowUtils {
  val alloc = new RootAllocator(Integer.MAX_VALUE)

  // Write to Arrow Vectors
  def writeVector[A](vec: FieldVector, data: Chunk[A]): Unit = {
    val len = data.length

    vec.setValueCount(len)
    val vtype = vec.getMinorType

    vtype match {

      case TINYINT => {
        println("WR TINYINT vector")

        for (i <- 0 until len)
          vec.asInstanceOf[TinyIntVector].set(i, data(i).asInstanceOf[Int])
      }

      case FLOAT4 => {
        println("WR FLOAT4 vector")

        for (i <- 0 until len)
          vec.asInstanceOf[Float4Vector].set(i, data(i).asInstanceOf[Float])
      }

      case FLOAT8 => {
        println("WR FLOAT8 vector")

        for (i <- 0 until len)
          vec.asInstanceOf[Float8Vector].set(i, data(i).asInstanceOf[Double])
      }

      case VARCHAR => {
        println("WR VARCHAR vector")

        for (i <- 0 until len) {
          val tmp = data(i).asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
          // vec.asInstanceOf[VarCharVector].set(i, data(i).asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
          vec.asInstanceOf[VarCharVector].set(i, tmp)
        }

      }

      case _ => throw new Exception(s"Not yet implemented: $vtype")
    }

  }

  // Read from Arrow Vectors
  def readVector[A](vec: FieldVector): Chunk[A] = {

    println(s"input field fector: $vec")

    val out = {
      val vtype = vec.getMinorType
      val count = vec.getValueCount

      vtype match {

        case TINYINT => {
          println("RD TINYINT vector")

          val tmp = scala.collection.mutable.ArrayBuffer[Byte]()
          for (i <- 0 until count)
            if (!vec.isNull(i))
              tmp += vec.asInstanceOf[TinyIntVector].get(i)

          println(tmp)
          tmp
        }

        case FLOAT4 => {
          println("RD FLOAT4 vector")

          val tmp = scala.collection.mutable.ArrayBuffer[Float]()
          for (i <- 0 until count)
            if (!vec.isNull(i))
              tmp += vec.asInstanceOf[Float4Vector].get(i)
          tmp
        }

        case FLOAT8 => {
          println("RD FLOAT8 vector")

          val tmp = scala.collection.mutable.ArrayBuffer[Double]()
          for (i <- 0 until count)
            if (!vec.isNull(i))
              tmp += vec.asInstanceOf[Float8Vector].get(i)

          tmp
        }

        case VARCHAR => {
          println("RD VARCHAR vector")

          val tmp = scala.collection.mutable.ArrayBuffer[BArr]()
          for (i <- 0 until count)
            if (!vec.isNull(i))
              tmp += vec.asInstanceOf[VarCharVector].get(i)

          // println (s"Inside Utils. Buffer 0 : ${tmp(0).toString}")
          tmp
        }

        case _ => throw new Exception(s"Not yet implemented: $vtype")
      }

    }

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
