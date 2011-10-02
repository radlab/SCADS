import sbt._
import Keys._

object ScadsBuild extends Build {
  val buildVersion = "2.1.2-SNAPSHOT"
  val defaultScalaVersion = "2.9.1"

  val buildSettings = Defaults.defaultSettings ++ GhPages.ghpages.settings ++ Seq(
    organization := "edu.berkeley.cs",
    scalaVersion := defaultScalaVersion,
    version := buildVersion,
    shellPrompt := ShellPrompt.buildShellPrompt,
    resolvers := Seq(radlabRepo, localMaven),
    GhPages.ghpages.gitRemoteRepo := "git@github.com:radlab/SCADS.git",
    parallelExecution in Test := false,
    libraryDependencies += "org.scala-tools.sxr" %% "sxr" % "0.2.8-SNAPSHOT" % "plugin",
    ivyConfigurations += Configurations.CompilerPlugin,
    /* HACK work around due to bugs in sbt compiler plugin handling code */
    scalacOptions <++= (update, scalaSource in Compile) map {
      (report, source) =>
        val pluginClasspath = report matching configurationFilter(Configurations.CompilerPlugin.name)
        pluginClasspath.map("-Xplugin:" + _.getAbsolutePath).toSeq :+ "-deprecation" :+ "-unchecked" :+ "-Yrepl-sync" :+ ("-P:sxr:base-directory:" + source.getAbsolutePath)
    },
    /* HACK to work around broken ~ in 0.11.0 */
    watchTransitiveSources <<=
      Defaults.inDependencies[Task[Seq[File]]](
        watchSources.task, const(std.TaskExtra.constant(Nil)), aggregate = true, includeRoot = true) apply {
      _.join.map(_.flatten)
    })

  val deploySettings = buildSettings ++ DeployConsole.deploySettings

  addCompilerPlugin(avroPluginDep)

  /* Artifact Repositories */
  val localMaven = "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
  val radlabRepo = "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"

  /* Aggregator Project */
  lazy val scads = Project("scads", file("."), settings = buildSettings) aggregate (config, avroPlugin, comm, deploylib, scalaEngine, piql, scadr, tpcw, modeling)

  /* Avro Scala Compiler Plugin */
  lazy val avroPlugin = Project("avro-plugin", file("avro"), settings = buildSettings ++ Seq(libraryDependencies ++= avroPluginDeps))

  /* SCADS Core Projects */
  lazy val config = Project(
    "config",
    file("config"),
    settings = buildSettings ++ Seq(libraryDependencies ++= configDeps))

  lazy val comm = Project(
    "communication",
    file("comm"),
    settings = buildSettings ++ Seq(libraryDependencies ++= commDeps)
  ) dependsOn (config)

  lazy val optional = Project(
    "optional",
    file("optional"),
    settings = buildSettings ++ Seq(libraryDependencies ++= Seq(paranamer)))

  lazy val deploylib = Project(
    "deploylib",
    file("deploylib"),
    settings = deploySettings ++ Seq(
      libraryDependencies ++= deploylibDeps)
  ) dependsOn (comm, optional)

  lazy val scalaEngine = Project(
    "scala-engine",
    file("scalaengine"),
    settings = deploySettings ++ Seq(
      libraryDependencies ++= scalaEngineDeps)
  ) dependsOn (config, comm, deploylib)

  lazy val piql = Project(
    "piql", file("piql"),
    settings = deploySettings ++ Seq(
      libraryDependencies ++= useAvroPlugin)
  ) dependsOn (config, comm, scalaEngine)

  lazy val perf = Project(
    "perf",
    file("perf"),
    settings = deploySettings ++ Seq(
      libraryDependencies ++= useAvroPlugin,
      initialCommands in console += (
        "import edu.berkeley.cs._\n" +
          "import edu.berkeley.cs.avro._\n" +
          "import edu.berkeley.cs.scads.comm._\n" +
          "import edu.berkeley.cs.scads.storage._\n" +
          "import edu.berkeley.cs.scads.perf._"))
  ) dependsOn (config, comm, scalaEngine, deploylib)

  /* Other projects and experiments */
  lazy val modeling = Project(
    "modeling",
    file("experiments/modeling"),
    settings = deploySettings ++ Seq(
      libraryDependencies ++= useAvroPlugin,
      initialCommands in console += (
        "import edu.berkeley.cs.scads.piql.modeling._\n" +
          "import edu.berkeley.cs.scads.piql.modeling.Experiments._")
    )
  ) dependsOn (piql, perf, deploylib, scadr, tpcw)

  lazy val scadr = Project(
    "scadr",
    file("piql/scadr"),
    settings = deploySettings ++ Seq(libraryDependencies ++= useAvroPlugin)
  ) dependsOn (piql % "compile;test->test", perf)

  lazy val tpcw = Project(
    "tpcw",
    file("piql/tpcw"),
    settings = deploySettings ++ Seq(libraryDependencies ++= useAvroPlugin)
  ) dependsOn (piql, perf)

  lazy val axer = Project(
    "axer",
    file("axer"),
    settings = deploySettings ++ Seq(libraryDependencies ++= useAvroPlugin))

  lazy val matheon = Project(
    "matheon",
    file("matheon"),
    settings = deploySettings ++ Seq(libraryDependencies ++= useAvroPlugin)
  ) dependsOn (config, comm, scalaEngine)

  /**
   * Dependencies
   */

  def configDeps = configgy +: testDeps //Note: must be a def to avoid null pointer exception
  val configgy = "net.lag" % "configgy" % "2.0.0"

  def testDeps = Seq(scalaTest, junit)

  val scalaTest = "org.scalatest" %% "scalatest" % "1.6.1"
  val junit = "junit" % "junit" % "4.7"

  def avroPluginDeps = Seq(avroJava, avroIpc, scalaCompiler, configgy) ++ testDeps

  val avroJava = "org.apache.avro" % "avro" % "1.6.0-SNAPSHOT"
  val avroIpc = "org.apache.avro" % "avro-ipc" % "1.6.0-SNAPSHOT"
  val scalaCompiler = "org.scala-lang" % "scala-compiler" % defaultScalaVersion
  val avroPluginDep = "edu.berkeley.cs" %% "avro-plugin" % buildVersion % "plugin"
  val avroPluginCompile = "edu.berkeley.cs" %% "avro-plugin" % buildVersion
  val paranamer = "com.thoughtworks.paranamer" % "paranamer" % "2.0"

  def useAvroPlugin = Seq(avroPluginDep, avroPluginCompile)

  def commDeps = Seq(netty, zookeeper, commonsHttpClient, log4j, avroPluginDep, avroPluginCompile) ++ testDeps

  val netty = "org.jboss.netty" % "netty" % "3.2.1.Final"
  val log4j = "log4j" % "log4j" % "1.2.15"
  val zookeeper = "org.apache.zookeeper" % "zookeeper" % "3.3.1"

  def scalaEngineDeps = Seq(bdb, avroPluginDep, avroPluginCompile) ++ testDeps

  val bdb = "com.sleepycat" % "je" % "4.0.71"

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


  def replDeps = Seq(lift, jetty6, servlet, sl4jConfiggy)

  val lift = "net.liftweb" %% "lift-mapper" % "2.2-SNAPSHOT" % "compile"
  val jetty6 = "org.mortbay.jetty" % "jetty" % "6.1.25" % "test"
  val servlet = "javax.servlet" % "servlet-api" % "2.5" % "provided"
  val sl4jConfiggy = "com.notnoop.logging" % "slf4j-configgy" % "0.0.1"
}

/**
 * Mixin to create prompt of the form [current project]:[current git branch]>
 */
object ShellPrompt {

  object devnull extends ProcessLogger {
    def info(s: => String) {}

    def error(s: => String) {}

    def buffer[T](f: => T): T = f
  }

  val current = """\*\s+(\w+)""".r

  def gitBranches = ("git branch --no-color" lines_! devnull mkString)

  val buildShellPrompt = {
    (state: State) => {
      val currBranch = try current findFirstMatchIn gitBranches map (_ group (1)) getOrElse "-" catch {
        case e => "noBranch"
      }
      val currProject = Project.extract(state).currentProject.id
      "%s:%s> ".format(currProject, currBranch)
    }
  }
}

object DeployConsole extends BuildCommon {

  import Classpaths._
  import sbt.Project.Initialize

  val packageDependencies = TaskKey[Seq[java.io.File]]("package-dependencies", "get package deps")
  val deployConsole = TaskKey[Unit]("deploy-console", "scala console for deploying to EC2")

  def findPackageDeps: Initialize[Task[Seq[java.io.File]]] =
    (thisProjectRef, thisProject, settings) flatMap {
      (projectRef: ProjectRef, project: ResolvedProject, data: Settings[Scope]) => {
        def visit(p: ProjectRef): Seq[Task[java.io.File]] = {
          val depProject = thisProject in p get data getOrElse sys.error("Invalid project: " + p)
          val jarFile = (Keys.`package` in (p, ConfigKey("runtime"))).get(data).get
          jarFile +: depProject.dependencies.map {
            case ResolvedClasspathDependency(dep, confMapping) => dep
          }.flatMap(visit).toList
        }
        visit(projectRef).join.map(_.toSet.toSeq)
      }
    }

  val deploySettings = Seq(
    packageDependencies <<= findPackageDeps,
    deployConsole <<= (packageDependencies, fullClasspath in Runtime, compilers in console, scalacOptions in console, initialCommands in console, streams) map {
      (deps: Seq[java.io.File], cp: Classpath, cs, options, initCmds, s) => {
        val allJars = deps ++ cp.files.filter(_.getName endsWith "jar")
        val cmds = Seq(
          "import deploylib._",
          "import deploylib.ec2._",
          "import deploylib.mesos._",
          "val allJars = " + allJars.map(f => "new java.io.File(\"%s\")".format(f.getCanonicalPath)).mkString("Seq(", ",", ")"),
          "deploylib.mesos.MesosCluster.jarFiles = allJars",
          "implicit val cluster = new Cluster()",
          "implicit def zooKeeperRoot = cluster.zooKeeperRoot",
          "implicit def classSource = cluster.classSource",
          "implicit def serviceScheduler = cluster.serviceScheduler"
        ).mkString("\n") + "\n" + initCmds

        (new Console(cs.scalac))(Build.data(cp), options, cmds, s.log).foreach(msg => error(msg))
        println()
      }
    }
  )
}
