import sbt._
import xsbt.ScalaInstance

import java.io.File

class ScadsProject(info: ProjectInfo) extends ParentProject(info) {

  abstract class ScadsSubProject(info: ProjectInfo) extends DefaultProject(info) with AvroCompilerPlugin with AssemblyBuilder {
    override def fork = forkRun("-Xmx4G" :: Nil)

    //HACK
    val bdb = "com.sleepycat" % "je" % "4.0.71"
    val optional = "optional" %% "optional" % "0.1"
  }

  class Config(info: ProjectInfo) extends DefaultProject(info) {
    val configgy = "net.lag" % "configgy" % "2.0.0"
    val scalaTest = "org.scalatest" % "scalatest" % "1.2"
    val junit = "junit" % "junit" % "4.7"
  }
 class AvroPlugin(info: ProjectInfo) extends DefaultProject(info) {
    val avroJava = "org.apache.hadoop" % "avro" % "1.3.3"
    val configgy = "net.lag" % "configgy" % "2.0.0"
  }
 class Comm(info: ProjectInfo) extends ScadsSubProject(info) {
    val netty = "org.jboss.netty" % "netty" % "3.2.1.Final"
    val zookeeper = "org.apache.hadoop.zookeeper" % "zookeeper" % "3.3.1"
    val log4j = "log4j" % "log4j" % "1.2.15"
  }
  class Piql(info: ProjectInfo) extends ScadsSubProject(info)
  class ScalaEngine(info: ProjectInfo) extends ScadsSubProject(info){
  }
  class Perf(info: ProjectInfo) extends ScadsSubProject(info) {
    val deploylib = "edu.berkeley.cs" %% "deploylib" % "2.1.0-SNAPSHOT"
  }

  lazy val config      = project("config", "config", new Config(_))
  lazy val avro        = project("avro", "avro-plugin", new AvroPlugin(_))
  lazy val comm        = project("comm", "communication", new Comm(_), config, avro)
  lazy val scalaengine = project("scalaengine", "storage-engine", new ScalaEngine(_), config, avro, comm)
  lazy val piql        = project("piql", "piql", new Piql(_), config, avro, comm, scalaengine)
  lazy val perf        = project("perf", "performance", new Perf(_), config, avro, comm, scalaengine, piql)

  //PIQL Apps
  class Scadr(info: ProjectInfo) extends ScadsSubProject(info) {
  }
  lazy val scadr       = project("piql" / "scadr", "scadr", new Scadr(_), piql)

  val radlabRepo = "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"
  override def managedStyle = ManagedStyle.Maven
  val publishTo = "Radlab Snapshots" at "http://scads.knowsql.org/nexus/content/repositories/snapshots/"
  Credentials(Path.userHome / ".ivy2" / ".credentials", log)
}

trait AvroCompilerPlugin extends AutoCompilerPlugins {
  override def getScalaInstance(version: String) = { 
    val pluginJars = compileClasspath.filter(path => pluginDeps.contains(path.name)).getFiles.toSeq
    withExtraJars(super.getScalaInstance(version), pluginJars) 
  }
    
   private val pluginDeps = Set("avro-1.3.3.jar", "jackson-core-asl-1.4.2.jar", "jackson-mapper-asl-1.4.2.jar")

   private def withExtraJars(si: ScalaInstance, extra: Seq[File]) =
     ScalaInstance(si.version, si.libraryJar, si.compilerJar, info.launcher, extra : _*)


  val avroPlugin = compilerPlugin("edu.berkeley.cs.scads" %% "avro-plugin" % "2.1.0-SNAPSHOT")
  val pluginDepenency = "edu.berkeley.cs.scads" %% "avro-plugin" % "2.1.0-SNAPSHOT"
}
