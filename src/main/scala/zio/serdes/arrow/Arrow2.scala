package zio.serdes.arrow

import org.apache.arrow.vector.VectorSchemaRoot
import java.io.ByteArrayInputStream
import org.apache.arrow.vector.ipc.ArrowStreamReader

import zio.serdes.Types._
import zio.serdes.Serdes2
import ArrowUtils._

object Serd2 {

  val Arrow2Serdes: Serdes2[Chunk2Schema] = new Serdes2[Chunk2Schema] {

    def serialize[A, B](din: Chunk2Schema[A, B]): BArr = {

      // Unpack data and schema
      val (din0, din1, schema) = din

      // Write setup
      val numBatches = 1
      val numVectors = schema.getFields.size

      //Create a root alloc for this schema
      val root = VectorSchemaRoot.create(schema, alloc)

      for (i <- 0 until numVectors)
        root.getFieldVectors.get(i).allocateNew

      // Write to vectors
      writeVector(root, din0)
      writeVector(root, din1)

      // Write to output stream
      writeStream(root, numBatches)

    }

    def deserialize[A, B](din: BArr): Chunk2Schema[A, B] = {

      val stream = new ByteArrayInputStream(din)
      val reader = new ArrowStreamReader(stream, alloc)

      val root   = reader.getVectorSchemaRoot
      val schema = root.getSchema
      val vec0   = root.getFieldVectors.get(0)
      val vec1   = root.getFieldVectors.get(1)

      // Read vectors
      reader.loadNextBatch
      val out0 = readVector(vec0)
      val out1 = readVector(vec1)

      println(s"out0 = $out0")
      println(s"out1 = $out1")
      (out0, out1, schema)

    }

  }

}
