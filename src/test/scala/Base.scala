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
    scatter and gather byte array               $sgBArr
    serialize and deserialize byte array        $sdBArr
    serialize and deserialize an Arrow          $sdArrow
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

  def sdBArr = {

    val arr: Array[Int] = Array(1, 2, 3)
    val chunk           = Chunk.fromArray(arr)

    val bytes = ChunkSerdes.serialize[Int](chunk)
    val out   = ChunkSerdes.deserialize[Int](bytes)

    chunk === out
  }

  def sdArrow = {

    val testSchema = new Schema(
      asList(new Field("testField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()))
    )

    val din = Chunk(1, 2, 3, 5)

    val bytes = ArrowSerdes.serialize((din, testSchema))
    val dout  = ArrowSerdes.deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == testSchema

  }

}
