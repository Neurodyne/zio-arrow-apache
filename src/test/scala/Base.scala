package nettest

import org.specs2._
import zio.{ Chunk, DefaultRuntime }

import zio.serdes._

class BaseSpec extends Specification with DefaultRuntime {

  def is = s2"""

  ZIO Serdes should
    scatter and gather byte array               $sgBArr
    serialize and deserialize byte array        $sdBArr
    serialize and deserialize Apache Arrow      $sdArrow
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

    val bytes = Serdes[Chunk, Chunk].serialize[Int](chunk)
    val out   = Serdes[Chunk, Chunk].deserialize[Int](bytes)

    chunk === out
  }

  def sdArrow = {
    import org.apache.arrow.vector.types.pojo.{ ArrowType, Field, FieldType, Schema }
    import org.apache.arrow.vector.{ VectorSchemaRoot }
    import org.apache.arrow.memory.RootAllocator
    import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
    import org.apache.arrow.vector.ipc.{ ArrowStreamReader, ArrowStreamWriter }

    import java.util.Arrays.asList
    import java.util.Collections

    val allocator = new RootAllocator(128)

    // Serdes A Simple Array

    val arr: BArr = Array(1, 2, 3)

    val bytes = Serdes[ByteArrow, Array].deserialize(arr)
    val res0  = Serdes[ByteArrow, Array].serialize(bytes)

    res0 === arr

    // Serdes A Simple Array
    val schema = new Schema(
      asList(new Field("testField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.emptyList()))
    )

    val root: VectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)

    // Write the stream
    val res1: ByteArrayOutputStream = new ByteArrayOutputStream()
    val writer: ArrowStreamWriter   = new ArrowStreamWriter(root, null, res1)
    writer.close();

    // check output stream size
    res1.size must be_>(0)

    // Read the stream
    val in     = new ByteArrayInputStream(res1.toByteArray())
    val reader = new ArrowStreamReader(in, allocator)

    // Check schema
    (schema === reader.getVectorSchemaRoot.getSchema) and
      // Empty should return false
      (reader.loadNextBatch must beFalse) and
      (reader.getVectorSchemaRoot.getRowCount === 0)

  }
}
