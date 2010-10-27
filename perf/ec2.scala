implicit val scheduler = LocalExperimentScheduler(System.getProperty("user.name") + " console", "1@10.123.6.101:5050")
implicit def classpath = Deploy.s3Classpath
implicit val ec2zoo = ZooKeeperNode("zk://mesos-ec2.knowsql.org/")
