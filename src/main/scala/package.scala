package object deploylib {
  object Config extends net.lag.configgy.Config {
    // read config data from $HOME/.deploylib
    private val configFile = new java.io.File(System.getProperty("user.home"), ".deploylib")
    if (configFile.exists)
      loadFile(configFile.getParent, configFile.getName)
  }

  implicit def toParallelSeq[A](itr: Iterable[A]): ParallelSeq[A] = new ParallelSeq(itr.toList)
}
