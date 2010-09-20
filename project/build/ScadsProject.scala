import sbt._
import xsbt.ScalaInstance

import java.io.File

class ScadsProject(info: ProjectInfo) extends ParentProject(info) {

  lazy val config      = project("config", "SCADS Config")

  lazy val comm        = pluginProject("comm", "SCADS Communication Manager", config)
  lazy val scalaengine = pluginProject("scalaengine", "SCADS Storange Engine", config, comm)
  lazy val mesos       = pluginProject("mesos", "SCADS Mesos Binding", config, scalaengine)
  lazy val perf        = pluginProject("perf", "SCADS Performance Tests", config, mesos)
  lazy val piql        = pluginProject("piql", "PIQL Compiler and Execution Engine", config, comm, scalaengine)

  private def pluginProject(path: Path, name: String, deps: Project*): Project =
    project(path, name, new PluginProject(_), deps : _*)

  class PluginProject(info: ProjectInfo) extends DefaultProject(info) {

    override def compileOptions = {
      val plugin = compileClasspath.getFiles.toList.filter(_.getName == "avro-scala-compiler-plugin-1.1-SNAPSHOT.jar").head
      CompileOption("-Xplugin:%s".format(plugin.getAbsolutePath)) :: super.compileOptions.toList
    }

    private val pluginDeps = Set("avro-1.3.3.jar", "jackson-core-asl-1.4.2.jar", "jackson-mapper-asl-1.4.2.jar")

    override def getScalaInstance(version: String) = { 
      val pluginJars = compileClasspath.filter(path => pluginDeps.contains(path.name)).getFiles.toSeq
      withExtraJars(super.getScalaInstance(version), pluginJars) 
    }
    
    private def withExtraJars(si: ScalaInstance, extra: Seq[File]) =
      ScalaInstance(si.version, si.libraryJar, si.compilerJar, info.launcher, extra : _*)

  }

  val radlabRepo = "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"
  val mavenLocal = "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"

}
