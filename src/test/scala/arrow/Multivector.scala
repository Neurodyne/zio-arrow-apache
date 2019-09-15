package arrow

import java.util.Arrays.asList
import java.util.Collections
import org.specs2.Specification
import zio.{ Chunk, DefaultRuntime }

import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
import org.apache.arrow.vector.types.FloatingPointPrecision

import zio.serdes.arrow.Serd2.Arrow2Serdes

class MultiSpec extends Specification with DefaultRuntime {

  def is = s2"""

  Arrow Serdes should
    write multiple vectors of a single type           $multiVecSingle
    write multiple vectors of different types         $multiVecMulti
    
    """

  // def multiVecSingle = {

  //   val schema = new Schema(
  //     asList(
  //       new Field("intField0", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()),
  //       new Field("intField1", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList())
  //     )
  //   )

  //   println(schema.getFields)

  //   val allBytes = scala.collection.mutable.ArrayBuffer[BArr]()

  //   for (i <- 0 until 1) {
  //     val data = Chunk(i)
  //     allBytes += Serdes[ChunkSchema].serialize((data, schema))
  //   }

  //   // val bytes = Serdes[ChunkSchema].serialize((din0, schema))
  //   val finalBytes = allBytes.toArray.flatten
  //   val dout       = Serdes[ChunkSchema].deserialize(finalBytes)

  //   val (outChunk, outSchema) = dout

  //   Chunk(finalBytes)

  //   println(outChunk.toArray.flatten)
  //   println(Chunk(finalBytes))

  //   allBytes.clear
  //   // outChunk ===
  //   // outSchema == schema
  //   true === true
  // }

  def multiVecSingle = {

    val schema = new Schema(
      asList(
        new Field("intField0", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()),
        new Field("intField1", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList())
      )
    )

    val din0 = Chunk(1, 2)
    val din1 = Chunk(-1)

    val bytes = Arrow2Serdes.serialize((din0, din1, schema))
    val dout  = Arrow2Serdes.deserialize(bytes)

    val (dout0, dout1, outSchema) = dout

    dout0 === din0 && dout1 === din1 && outSchema == schema
  }

  def multiVecMulti = {

    val precision = FloatingPointPrecision.SINGLE

    val schema = new Schema(
      asList(
        new Field("intField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()),
        new Field("floatField", FieldType.nullable(new ArrowType.FloatingPoint(precision)), Collections.emptyList())
      )
    )

    val din0 = Chunk(1, 2)
    val din1 = Chunk(5.0f)

    val bytes = Arrow2Serdes.serialize((din0, din1, schema))
    val dout  = Arrow2Serdes.deserialize(bytes)

    val (dout0, dout1, outSchema) = dout

    dout0 === din0 && dout1 === din1 && outSchema == schema
  }
}
