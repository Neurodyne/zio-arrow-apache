package nettest

import org.specs2._
import zio.{ Chunk, DefaultRuntime }

import zio.serdes._

class BaseSpec extends Specification with DefaultRuntime {

  def is = s2"""

  ZIO Serdes should
    scatter and gather byte array               $sgBArr
    serialize and deserialize byte array        $sdBArr
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

  def sdArrow =
    true === true
}
