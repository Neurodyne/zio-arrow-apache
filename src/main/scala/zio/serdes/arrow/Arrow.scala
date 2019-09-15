package zio.serdes.arrow

import org.apache.arrow.vector.VectorSchemaRoot
import java.io.ByteArrayInputStream
import org.apache.arrow.vector.ipc.ArrowStreamReader

import zio.serdes.Types._
import zio.serdes.Serdes
import ArrowUtils._

object Serd {

  implicit val ArrowSerdes: Serdes[ChunkSchema] = new Serdes[ChunkSchema] {

    def serialize[A](din: ChunkSchema[A]): BArr = {

      // Unpack data and schema
      val (data, schema) = din

      // Write setup
      val numBatches = 1
      val numVectors = 1

      val len = data.length // write vector length

      //Create a root alloc for this schema
      val root = VectorSchemaRoot.create(schema, alloc)
      root.setRowCount(len)
      val vectors = root.getFieldVectors

      for (i <- 0 until numVectors)
        vectors.get(i).allocateNew

      // Write to vectors

      vectors.forEach(vec => writeVector(vec, data))

      // Write to output stream
      writeStream(root, numBatches)

    }

    def deserialize[A](din: BArr): ChunkSchema[A] = {

      val stream = new ByteArrayInputStream(din)
      val reader = new ArrowStreamReader(stream, alloc)

      val root   = reader.getVectorSchemaRoot
      val schema = root.getSchema
      val vec0   = root.getFieldVectors.get(0)

      // Read vectors
      reader.loadNextBatch
      val out = readVector(vec0)

      (out, schema)

    }

  }
}
