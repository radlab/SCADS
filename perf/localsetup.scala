implicit val scheduler = LocalExperimentScheduler(System.getProperty("user.name"))
implicit val classpath = System.getProperty("java.class.path").split(":").map(j => new File(j).getCanonicalPath).map(p => ServerSideJar(p)).toSeq
