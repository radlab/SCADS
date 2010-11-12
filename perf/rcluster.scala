implicit val scheduler = LocalExperimentScheduler(System.getProperty("user.name") + " console", "1@mesos-master.millennium.berkeley.edu:5050", "/work/deploylib/java_executor")
implicit def classpath = Deploy.workClasspath
implicit val zookeeper = ZooKeeperNode("zk://zoo1.millennium.berkeley.edu/")
