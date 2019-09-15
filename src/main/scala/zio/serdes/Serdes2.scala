package zio.serdes

import zio.serdes.Types._

sealed abstract class Serdes2[F[_, _]] {

  def serialize[A, B](din: F[A, B]): BArr
  def deserialize[A, B](din: BArr): F[A, B]

}
