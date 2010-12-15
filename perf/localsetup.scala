val mesosExecutor = new File("../mesos/frameworks/deploylib/java_executor").getCanonicalPath
implicit val scheduler = LocalExperimentScheduler("Local Console", "1@" + java.net.InetAddress.getLocalHost.getHostAddress + ":5050", mesosExecutor)
implicit val classpath = System.getProperty("java.class.path").split(":").map(j => new File(j).getCanonicalPath).map(p => ServerSideJar(p)).toSeq
implicit val zookeeper = ZooKeeperHelper.getTestZooKeeper().root
