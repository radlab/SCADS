import sbt._
import Keys._

object ScadsBuild extends Build {
  val buildVersion      = "2.1.2-SNAPSHOT"
  val defaultScalaVersion = "2.9.1"
  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := "edu.berkeley.cs",
    scalaVersion := defaultScalaVersion,
    version      := buildVersion,
    shellPrompt  := ShellPrompt.buildShellPrompt,
    resolvers    := Seq(radlabRepo, localMaven),
    parallelExecution in Test := false,
    publishTo <<= (version) { version: String =>
      val nexus = "http://scads.knowsql.org/nexus/content/repositories/"
      if (version.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus+"snapshots/") 
      else                                   Some("releases" at nexus+"releases/")
    },
    credentials += Credentials(Path.userHome / ".ivy2" / "credentials"),
    ivyConfigurations += Configurations.CompilerPlugin,
    /* HACK work around due to bugs in sbt compiler plugin handling code */
    scalacOptions <++= update map { report =>
      val pluginClasspath = report matching configurationFilter(Configurations.CompilerPlugin.name)
      pluginClasspath.map("-Xplugin:" + _.getAbsolutePath).toSeq
    })

  addCompilerPlugin(avroPluginDep)
				
  val localMaven = "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"		
  val radlabRepo = "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"

  lazy val scads = Project("scads", file("."), settings=buildSettings) aggregate (config, avroPlugin, comm, deploylib, scalaEngine, piql, scadr, tpcw, modeling)

  lazy val avroPlugin = Project("avro-plugin", file("avro"), settings=buildSettings ++ Seq(libraryDependencies := avroPluginDeps))
  lazy val config = Project("config", file("config"), settings=buildSettings ++ Seq(libraryDependencies := configDeps))
  lazy val comm = Project("communication", file("comm"), settings=buildSettings ++ Seq(libraryDependencies := commDeps)) dependsOn(config)
  lazy val optional = Project("optional", file("optional"), settings=buildSettings ++ Seq(libraryDependencies := Seq(paranamer)))
  lazy val deploylib = Project("deploylib", file("deploylib"), settings=buildSettings ++ Seq(libraryDependencies := deploylibDeps)) dependsOn(comm, optional)
  lazy val scalaEngine = Project("scala-engine", file("scalaengine"), settings=buildSettings ++ Seq(libraryDependencies := scalaEngineDeps)) dependsOn(config, comm, deploylib)

  lazy val modeling = Project("modeling", file("modeling"), settings=buildSettings ++ Seq(libraryDependencies := useAvroPlugin)) dependsOn(piql, perf, deploylib, scadr, tpcw)
  lazy val piql = Project("piql", file("piql"), settings=buildSettings ++ Seq(libraryDependencies := useAvroPlugin)) dependsOn(config, comm, scalaEngine)
  lazy val perf = Project("perf", file("perf"), settings=buildSettings ++ Seq(libraryDependencies := useAvroPlugin)) dependsOn(config, comm, scalaEngine, deploylib)
  lazy val scadr = Project("scadr", file("piql/scadr"), settings=buildSettings ++ Seq(libraryDependencies := useAvroPlugin)) dependsOn(piql % "compile;test->test", perf)
  lazy val tpcw = Project("tpcw", file("piql/tpcw"), settings=buildSettings ++ Seq(libraryDependencies := useAvroPlugin)) dependsOn(piql, perf)
  lazy val axer = Project("axer", file("axer"), settings=buildSettings ++ Seq(libraryDependencies := useAvroPlugin))
  lazy val matheron = Project("matheron", file("matheron"), settings=buildSettings ++ Seq(libraryDependencies := useAvroPlugin)) dependsOn(config, comm, scalaEngine)

  /* Config */
  def configDeps = configgy +: testDeps //Note: must be a def to avoid null pointer exception
  val configgy = "net.lag" % "configgy" % "2.0.0"

  def testDeps = Seq(scalaTest, junit)
  val scalaTest = "org.scalatest" %% "scalatest" % "1.6.1"
  val junit = "junit" % "junit" % "4.7"

  /* Avro */
  def avroPluginDeps = Seq(avroJava, avroIpc, scalaCompiler, configgy) ++ testDeps
  val avroJava = "org.apache.avro" % "avro" % "1.5.2-SNAPSHOT"
  val avroIpc = "org.apache.avro" % "avro-ipc" % "1.5.2-SNAPSHOT"
  val scalaCompiler = "org.scala-lang" % "scala-compiler" % defaultScalaVersion
  val avroPluginDep = "edu.berkeley.cs" %% "avro-plugin" % buildVersion % "plugin"
  val avroPluginCompile = "edu.berkeley.cs" %% "avro-plugin" % buildVersion
  val paranamer = "com.thoughtworks.paranamer" % "paranamer" % "2.0"

  def useAvroPlugin = Seq(avroPluginDep, avroPluginCompile)

  /* Comm */
  def commDeps = Seq(netty, zookeeper, commonsHttpClient, log4j, avroPluginDep, avroPluginCompile) ++ testDeps
  val netty = "org.jboss.netty" % "netty" % "3.2.1.Final"
  val log4j = "log4j" % "log4j" % "1.2.15"
  val zookeeper = "org.apache.zookeeper" % "zookeeper" % "3.3.1"


  /* Scala Engine */
  def scalaEngineDeps = Seq(bdb, avroPluginDep, avroPluginCompile) ++ testDeps
  val bdb = "com.sleepycat" % "je" % "4.0.71"

  /* deploy lib */
  def deploylibDeps = Seq(mesos, protoBuff, staxApi, jaxbApi, json, awsSdk, ganymedSsh2, commonsLoggingApi, commonsHttpClient, jets3t, jetty, mysql, javaSysMon, avroPluginDep, avroPluginCompile)
  val mesos = "org.apache" % "mesos" % "1.1"
  val protoBuff = "com.google.protobuf" % "protobuf-java" % "2.3.0"
  val staxApi = "javax.xml.stream" % "stax-api" % "1.0"
  val jaxbApi = "javax.xml.bind" % "jaxb-api" % "2.1"
  val json = "org.json" % "json" % "20090211"
  val awsSdk = "com.amazonaws" % "aws-java-sdk" % "1.1.5"
  val ganymedSsh2 = "ch.ethz.ganymed" % "ganymed-ssh2" % "build210"
  val commonsLoggingApi = "commons-logging" % "commons-logging-api" % "1.1"
  val commonsHttpClient = "commons-httpclient" % "commons-httpclient" % "3.0.1"
  val jets3t = "net.java.dev.jets3t" % "jets3t" % "0.7.1"
  val jetty = "org.mortbay.jetty" % "jetty" % "6.1.6"
  val mysql = "mysql" % "mysql-connector-java" % "5.1.12"
  val javaSysMon = "github.jezhumble" % "javasysmon" % "0.3.3"


  /* repl */
  val lift = "net.liftweb" %% "lift-mapper" % "2.2-SNAPSHOT" % "compile"
  val jetty6 = "org.mortbay.jetty" % "jetty" % "6.1.25" % "test"
  val h2 = "com.h2database" % "h2" % "1.2.121" % "runtime"
  // alternately use derby
  // val derby = "org.apache.derby" % "derby" % "10.2.2.0" % "runtime"
  val servlet = "javax.servlet" % "servlet-api" % "2.5" % "provided"
  val sl4jConfiggy = "com.notnoop.logging" % "slf4j-configgy" % "0.0.1"
}

object ShellPrompt {

  object devnull extends ProcessLogger {
    def info (s: => String) {}
    def error (s: => String) { }
    def buffer[T] (f: => T): T = f
  }

  val current = """\*\s+(\w+)""".r

  def gitBranches = ("/opt/local/bin/git branch --no-color" lines_! devnull mkString)

  val buildShellPrompt = {
    (state: State) => {
      val currBranch = current findFirstMatchIn gitBranches map (_ group(1)) getOrElse "-"
      val currProject = Project.extract (state).currentProject.id
      "%s:%s> ".format (currProject, currBranch)
    }
  }
}
