package arrow

import org.specs2.Specification
import zio.{ Chunk, DefaultRuntime }

import zio.serdes._
import zio.serdes.Types._
import zio.serdes.arrow.Serd._

import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
import java.util.Arrays.asList
import java.util.Collections
import org.apache.arrow.vector.types.FloatingPointPrecision

class BaseSpec extends Specification with DefaultRuntime {

  def is = s2"""

  ZIO Serdes should
    work for byte array                 $sgBArr
    work for Chunk                      
    work for Arrow Chunk[Int]           $sdArrChunkInt
    work for Arrow Chunk[Float]         $sdArrChunkFloat
    work for Arrow Chunk[Double]        $sdArrChunkDouble
    work for Arrow Chunk[String]        $sdArrChunkString
    throw exception for unknown type    $sdUnknown
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

  // def sdChunk = {

  //   val arr: Array[Int] = Array(1, 2, 3)
  //   val chunk           = Chunk.fromArray(arr)

  //   val bytes = ChunkSerdes.serialize[Int](chunk)
  //   val out   = ChunkSerdes.deserialize[Int](bytes)

  //   chunk === out
  // }

  def sdArrChunkInt = {

    val schema = new Schema(
      asList(new Field("testField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()))
    )

    val din = Chunk(1, 2, 3, 5)

    val bytes = Serdes[ChunkSchema].serialize((din, schema))
    val dout  = Serdes[ChunkSchema].deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == schema

  }

  def sdArrChunkFloat = {

    val precision = FloatingPointPrecision.SINGLE

    val schema = new Schema(
      asList(
        new Field("testField", FieldType.nullable(new ArrowType.FloatingPoint(precision)), Collections.emptyList())
      )
    )

    val din = Chunk(1.0f, -1.0f)

    val bytes = Serdes[ChunkSchema].serialize((din, schema))
    val dout  = Serdes[ChunkSchema].deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == schema

  }

  def sdArrChunkDouble = {

    val precision = FloatingPointPrecision.DOUBLE

    val schema = new Schema(
      asList(
        new Field("testField", FieldType.nullable(new ArrowType.FloatingPoint(precision)), Collections.emptyList())
      )
    )

    val din = Chunk(1.0, -1.0)

    val bytes = Serdes[ChunkSchema].serialize((din, schema))
    val dout  = Serdes[ChunkSchema].deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == schema

  }

  def sdArrChunkString = {

    val schema = new Schema(
      asList(
        new Field("testField", FieldType.nullable(new ArrowType.Utf8), Collections.emptyList())
      )
    )

    val din = Chunk("one", "two")

    val bytes = Serdes[ChunkSchema].serialize((din, schema))
    val dout  = Serdes[ChunkSchema].deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == schema

  }

  def sdUnknown = {

    val schema = new Schema(
      asList(
        new Field("binaryField", FieldType.nullable(new ArrowType.Binary), Collections.emptyList())
      )
    )

    val din = Chunk(1, 0)

    val bytes = Serdes[ChunkSchema].serialize((din, schema))
    val dout  = Serdes[ChunkSchema].deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == schema

  }

}
