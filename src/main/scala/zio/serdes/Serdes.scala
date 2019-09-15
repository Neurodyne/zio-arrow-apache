package zio.serdes

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import zio.{ Task, UIO, ZIO, ZManaged }

import zio.serdes.Types._
import zio.DefaultRuntime

abstract class Serdes[F[_]] {

  def serialize[A](din: F[A]): BArr
  def deserialize[A](din: BArr): F[A]

}

// Helper methods for simple object serialization
object Serdes {
  val rt = new DefaultRuntime {}

  def apply[F[_]](implicit srd: Serdes[F]) = srd

  def scatter[F[_], A](value: F[A]): ByteArrayOutputStream = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos                           = new ObjectOutputStream(stream)

    val str = ZManaged.make(Task(oos.writeObject(value)))(_ => UIO(oos.close))

    rt.unsafeRun(str.use { _ =>
      ZIO.unit
    })

    stream
  }

  def gather[F[_], A](bytes: BArr): F[A] = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))

    val str = ZManaged.make(ZIO.unit)(_ => UIO(ois.close))

    rt.unsafeRun(str.use { _ =>
      ZIO.succeed(ois.readObject.asInstanceOf[F[A]])
    })

  }

}
