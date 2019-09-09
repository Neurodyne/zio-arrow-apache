package basetest

import org.specs2.Specification
import zio.{ Chunk, DefaultRuntime }

import zio.serdes._
import zio.serdes.serdes._

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

    val din = Chunk(1, 2, 3)

    val bytes = Serdes[Chunk, ArrStreamReader].serialize(din)
    val dout  = Serdes[Chunk, ArrStreamReader].deserialize(bytes)

    dout === din
    // true === true
  }
}
