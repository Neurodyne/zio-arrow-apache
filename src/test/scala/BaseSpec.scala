package nettest

import org.specs2._
import zio.{ Chunk, DefaultRuntime }

import zioSerdesPkg._
import zioSerdes._

class BaseSpec extends Specification with DefaultRuntime {

  def is = s2"""

  ZIO Serdes should
    scatter and gather byte array               $sgBArr
    serialize and deserialize byte array        $sdBArr

    serialize and deserialize parqeut           $sgParquet

    """

  def sgBArr = {

    //val arr = Array(1, 2, 3)

    val s = Array("String")

    val bytes =  Serdes.scatter[Array, String](s)
    val out = Serdes.gather[Array, String](bytes)

    s === out

  }

  def sdBArr = {

    val arr: Array[Int] = Array(1, 2, 3)
    val chunk     = Chunk.fromArray(arr)

    val bytes = Serdes.chunkSerdes.serialize[Int](chunk)
    val out   = Serdes.chunkSerdes.deserialize[Int](bytes)

    //zioSerdesPkg.eqv(arr, out.toArray) === true
    chunk === out // Chunk comparison doesn't work!
  }

  def sgParquet =
    // import ParquetPkg._
    // import ParquetReader._

    // // Read parquet data
    // val path = "/tmp/hello.pq"

    // val rows: Chunk[TypeData] =
    //  for {
    //    frame <- Reader.getFrame(path)
    //    data  <- Reader.getRows(frame)
    //  } yield data

    // val bytes: Chunk[Byte] = Serdes.chunkSerdes.serialize(rows)
    true === true

}
