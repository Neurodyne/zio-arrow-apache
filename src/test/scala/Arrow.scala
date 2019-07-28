package arrowtest

import org.specs2._
import zio.{ DefaultRuntime }

class ArrowSpec extends Specification with DefaultRuntime {

  def is = s2"""

  ZIO Serdes should
    serialize and deserialize arrow             $sdArrow

    """

  def sdArrow = {

    import java.nio.ByteBuffer

    import org.apache.arrow.memory.RootAllocator

    type BArr = Array[Byte]
    val arrLength = 64

    val allocator = new RootAllocator(128)

    val expecteds: BArr = Array.fill(arrLength)((scala.util.Random.nextInt(256) - 128).toByte)

    val data = ByteBuffer.wrap(expecteds)

    val buf = allocator.buffer(expecteds.length)
    buf.setBytes(0, data, 0, data.capacity())

    val actuals = new BArr(expecteds.length)
    buf.getBytes(0, actuals)
    expecteds === actuals

  }

}
