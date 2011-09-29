package deploylib

import ec2._
import rcluster._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.scads.comm._

import java.io.File

package object mesos {
  implicit def toFile(str: String) = new java.io.File(str)

  def classpath = System.getProperty("java.class.path").split(":").filter(_ endsWith "jar")
  def s3Classpath = classpath.map(f => S3CachedJar(S3Cache.getCacheUrl(new File(f)))).toSeq
  def codeS3Classpath = s3Classpath.map(j => """S3CachedJar("%s")""".format(j.url)).toList.toString

  def workClasspath = {
    classpath.map(jar => {
      val cacheLocation = r2.cacheFile(jar)
      ServerSideJar(cacheLocation.getCanonicalPath)
    }).toSeq
  }

  implicit object MsgHandler extends edu.berkeley.cs.scads.comm.ServiceRegistry[Message]
}
