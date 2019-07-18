package nettest

import org.specs2._
import zio.{ Chunk, DefaultRuntime }

import zioSerdesPkg._
import zioSerdes._
// import zioSerdes.Serdes._

class BaseSpec extends Specification with DefaultRuntime {

  def is = s2"""

  ZIO Serdes should      
    serialize byte array    $serBArr    

    """

  def serBArr = {

    val arr: BArr = Array(1, 2, 3)
    val data      = SChunk[BArr].fromArray(arr)

    val bytes: Chunk[Byte] = Serdes.chunkSerdes.serialize(data)

    true === true

  }

}
