package zioSerdes

import org.apache.commons.lang3.SerializationUtils
import simulacrum._

// sealed abstract class ZioSerdes[F[_]] {
@typeclass trait ZioSerdes[F[_]] {

  @op(">>>") def serialize[A, B](din: F[A]): F[B]
  @op("<<<") def deserialize[A, B](din: F[B]): F[A]
}

// object ZioSerdes {
//   implicit val

// }
