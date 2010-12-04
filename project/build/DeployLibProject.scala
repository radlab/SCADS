import sbt._
import xsbt.ScalaInstance

import java.io.File

class DeployLibProject(info: ProjectInfo) extends DefaultProject(info) {

  val mesos = "edu.berkeley.cs.mesos" % "java" % "1.0"
  val communication = "edu.berkeley.cs.scads" %% "communication" % "2.1.0-SNAPSHOT"
  val optional = "optional" %% "optional" %  "0.1"
  val configgy = "net.lag" % "configgy" % "2.0.0"
  val staxApi = "javax.xml.stream" % "stax-api" % "1.0"
  val jaxbApi = "javax.xml.bind" % "jaxb-api" % "2.1"
  val json = "org.json" % "json" % "20090211"
  val ec2 = "com.amazonaws" % "ec2" % "20090404"
  val ganymedSsh2 = "ch.ethz.ganymed" % "ganymed-ssh2" % "build210"
  val commonsLoggingApi = "commons-logging" % "commons-logging-api" % "1.1"
  val commonsHttpClient = "commons-httpclient" % "commons-httpclient" % "3.0.1"
  val jets3t = "net.java.dev.jets3t" % "jets3t" % "0.7.1"
  val jetty = "org.mortbay.jetty" % "jetty" % "6.1.6"


  val radlabRepo = "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/"
  override def managedStyle = ManagedStyle.Maven
  val publishTo = "Radlab Snapshots" at "http://scads.knowsql.org/nexus/content/repositories/snapshots/"
  Credentials(Path.userHome / ".ivy2" / ".credentials", log)
}
