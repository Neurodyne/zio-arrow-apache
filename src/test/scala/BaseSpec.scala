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

    serialize and deserialize parqeut           

    """

  def sgBArr = {

    val arr: BArr = Array(1, 2, 3)

    val bytes: BArr = Serdes.scatter(arr)
    val out: BArr   = Serdes.gather(bytes)

    eqv(arr, out) === true

  }

  def sdBArr = {

    val arr: BArr = Array(1, 2, 3)
    val chunk     = Chunk.fromArray(arr)
    val data      = StreamData(chunk)

    val bytes: Chunk[Byte]    = Serdes.chunkSerdes.serialize(data)
    val out: StreamData[Byte] = Serdes.chunkSerdes.deserialize(bytes)

    zioSerdesPkg.eqv(arr, out.din.toArray) === true
    //Chunk(arr) === out.din // Chunk comparison doesn't work!
  }

  def serParquet =
    //import ParquetPkg._
    //import ParquetReader._

    //// Read parquet data
    //val path = "/tmp/hello.pq"

    //val rows: Chunk[TypeData] =
    //  for {
    //    frame <- Reader.getFrame(path)
    //    data  <- Reader.getRows(frame)
    //  } yield data

    //val bytes: Chunk[Byte] = Serdes.chunkSerdes.serialize(rows)
    true === true

}
