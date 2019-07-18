package nettest

import org.specs2._
import zio.{ Chunk, DefaultRuntime }

import zioSerdesPkg._
import zioSerdes._

class BaseSpec extends Specification with DefaultRuntime {

  def is = s2"""

  ZIO Serdes should      
    serialize byte array    $serBArr    

    """

  def serBArr = {

    val arr: BArr = Array(1, 2, 3)
    val chunk     = Chunk.fromArray(arr)
    val data      = streamData(chunk)

    val bytes: Chunk[Byte] = Serdes.chunkSerdes.serialize(data)

    true === true

  }

}
