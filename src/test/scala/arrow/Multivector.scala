package arrow

import java.util.Arrays.asList
import java.util.Collections
import org.specs2.Specification
import zio.{ Chunk, DefaultRuntime }

import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
import org.apache.arrow.vector.types.FloatingPointPrecision

import zio.serdes._
import zio.serdes.Types._
import zio.serdes.arrow.Serd._

class MultiSpec extends Specification with DefaultRuntime {

  def is = s2"""

  Arrow Serdes should
    write multiple vectors of a single type $multivecSingle
    
    """

  def multivecSingle = {

    val precision = FloatingPointPrecision.SINGLE

    val testSchema = new Schema(
      asList(
        new Field("intField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()),
        new Field("floatField", FieldType.nullable(new ArrowType.FloatingPoint(precision)), Collections.emptyList())
      )
    )

    val intdata = Chunk(1, 2)
    Chunk(5.0f)
    // val din       = (intdata, floatdata)
    val din = intdata

    val bytes = Serdes[ChunkSchema].serialize((din, testSchema))
    val dout  = Serdes[ChunkSchema].deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == testSchema
    // true === true
  }
}
