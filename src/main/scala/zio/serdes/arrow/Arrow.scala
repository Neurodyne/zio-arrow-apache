package zio.serdes.arrow

import org.apache.arrow.vector.VectorSchemaRoot
import java.io.ByteArrayInputStream
import org.apache.arrow.vector.ipc.ArrowStreamReader

import zio.serdes.serdes._
import zio.serdes.Serdes
import ArrowUtils._

object ArrowSerdes extends Serdes[ChunkSchema] {

  def serialize[A](din: ChunkSchema[A]): BArr = {

    // Unpack data and schema
    val (data, schema) = din

    // Write setup
    val numBatches = 1
    data.length // write vector length

    //Create a root alloc for this schema
    val root = VectorSchemaRoot.create(schema, alloc)

    for (i <- 0 until numBatches)
      root.getFieldVectors.get(i).allocateNew

    // Write to vectors
    // unsafeRun(ZIO.effectTotal(writeVectors(root, data)))
    writeVectors(root, data)

    // Write to output stream
    writeStream(root, numBatches)

  }

  def deserialize[A](din: BArr): ChunkSchema[A] = {

    val stream = new ByteArrayInputStream(din)
    val reader = new ArrowStreamReader(stream, alloc)

    val root   = reader.getVectorSchemaRoot
    val schema = root.getSchema

    // Read vectors
    reader.loadNextBatch
    val out = readVectors(root)

    (out, schema)

  }

}
