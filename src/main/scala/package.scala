package object deploylib {
  object Config extends net.lag.configgy.Config {

  }

  implicit def toParallelSeq[A](itr: Iterable[A]): ParallelSeq[A] = new ParallelSeq(itr.toList)
}
