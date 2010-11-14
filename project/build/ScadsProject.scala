import sbt._
import xsbt.ScalaInstance

import java.io.File

class ScadsProject(info: ProjectInfo) extends ParentProject(info) {
  class Config(info: ProjectInfo) extends DefaultProject(info) {
    val configgy = "net.lag" % "configgy" % "2.0.0"
  }
 class AvroPlugin(info: ProjectInfo) extends DefaultProject(info) {
    val avroJava = "org.apache.hadoop" % "avro" % "1.3.3"
    val configgy = "net.lag" % "configgy" % "2.0.0"
  }
 class Comm(info: ProjectInfo) extends DefaultProject(info) with AvroCompilerPlugin {
    val netty = "org.jboss.netty" % "netty" % "3.2.1.Final"
    val zookeeper = "org.apache.hadoop.zookeeper" % "zookeeper" % "3.3.1"
    val log4j = "log4j" % "log4j" % "1.2.15"
  }
  class Piql(info: ProjectInfo) extends DefaultProject(info) with AvroCompilerPlugin
  class ScalaEngine(info: ProjectInfo) extends DefaultProject(info) with AvroCompilerPlugin{
    val bdb = "com.sleepycat" % "je" % "4.0.71"
    val optional = "optional" % "optional" % "1.0"
  }
  class Perf(info: ProjectInfo) extends DefaultProject(info) with AvroCompilerPlugin{
    val deploylib = "edu.berkeley.cs" % "deploy" % "2.1-SNAPSHOT"
  }

  lazy val config      = project("config", "Config", new Config(_))
  lazy val avro        = project("avro", "Avro Scala Compiler Plugin", new AvroPlugin(_))
  lazy val comm        = project("comm", "Communication Manager", new Comm(_), config, avro)
  lazy val scalaengine = project("scalaengine", "Storage Engine", new ScalaEngine(_), config, avro, comm)
  lazy val piql        = project("piql", "PIQL Compiler and Execution Engine", new Piql(_), config, avro, comm, scalaengine)
  lazy val perf        = project("perf", "Performance Tests", new Perf(_), config, avro, comm, scalaengine, piql)

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


  val avroPlugin = compilerPlugin("edu.berkeley.cs.scads" %% "avro-scala-compiler-plugin" % "2.1.0-SNAPSHOT")
  val pluginDepenency = "edu.berkeley.cs.scads" %% "avro-scala-compiler-plugin" % "2.1.0-SNAPSHOT"
}
