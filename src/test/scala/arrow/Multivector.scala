package arrow

import org.specs2.Specification
import zio.{ Chunk, DefaultRuntime }

import zio.serdes.arrow.ArrowSerdes

import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
import java.util.Arrays.asList
import java.util.Collections
import org.apache.arrow.vector.types.FloatingPointPrecision

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

    val din = Chunk(1, 2, 3, 5)

    val bytes = ArrowSerdes.serialize((din, testSchema))
    val dout  = ArrowSerdes.deserialize(bytes)

    val (outChunk, outSchema) = dout

    outChunk === din && outSchema == testSchema
  }
}
