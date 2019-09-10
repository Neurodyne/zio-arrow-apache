package basetest

import org.specs2.Specification
import zio.{ Chunk, DefaultRuntime }

import zio.serdes._
import zio.serdes.arrow.ArrowSerdes

import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
import java.util.Arrays.asList
import java.util.Collections

class BaseSpec extends Specification with DefaultRuntime {

  def is = s2"""

  ZIO Serdes should
    work for byte array                 $sgBArr
    work for Chunk                      $sdChunk
    work for Arrow Chunk[Int]           $sdArrChunkInt
    work for Arrow Chunk[Float]         $sdArrChunkFloat
    """

  def sgBArr = {

    val arr      = Array(1, 2, 3)
    val bytesArr = Serdes.scatter[Array, Int](arr).toByteArray
    val outArr   = Serdes.gather[Array, Int](bytesArr)

    arr == outArr

    val s     = Array("String")
    val bytes = Serdes.scatter[Array, String](s).toByteArray
    val out   = Serdes.gather[Array, String](bytes)

    s === out

  }

  def sdChunk = {

    val arr: Array[Int] = Array(1, 2, 3)
    val chunk           = Chunk.fromArray(arr)

    val bytes = ChunkSerdes.serialize[Int](chunk)
    val out   = ChunkSerdes.deserialize[Int](bytes)

    chunk === out
  }

  def sdArrChunkInt = {

    val testSchema = new Schema(
      asList(new Field("testField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()))
    )

    val din = Chunk(1, 2, 3, 5)

    val bytes = ArrowSerdes.serialize((din, testSchema))
    val dout  = ArrowSerdes.deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == testSchema

  }

  def sdArrChunkFloat = {

    val testSchema = new Schema(
      asList(new Field("testField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()))
    )

    val din = Chunk(1.0, 2.0)

    val bytes = ArrowSerdes.serialize((din, testSchema))
    val dout  = ArrowSerdes.deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == testSchema

  }

}
