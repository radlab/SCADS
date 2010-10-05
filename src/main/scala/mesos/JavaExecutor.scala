package deploylib
package mesos

import java.io.{File, InputStream, BufferedReader, InputStreamReader, FileOutputStream}
import java.net.URL

import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import net.lag.logging.Logger

import ec2._
import edu.berkeley.cs.avro.marker._


import _root_.mesos._

sealed trait ClassSource extends AvroUnion
case class ServerSideJar(var path: String) extends AvroRecord with ClassSource
protected case class S3CachedJar(var url: String) extends AvroRecord with ClassSource

object S3CachedJar {
  def apply(jarLocation: File): S3CachedJar = {
    new S3CachedJar(S3Cache.getCacheUrl(jarLocation))
  }
}

object JvmProcess {def apply(bytes: Array[Byte]) = classOf[JvmProcess].newInstance.parse(bytes)}
case class JvmProcess(var classpath: List[ClassSource], var mainclass: String, var args: List[String], var props: Map[String, String] = Map.empty) extends AvroRecord

object JavaExecutor {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("mesos")
    val driver = new MesosExecutorDriver(new JavaExecutor())
    driver.run()
  }
}

class StreamTailer(stream: InputStream, size: Int = 100) extends Runnable {
  val reader = new BufferedReader(new InputStreamReader(stream))
  val thread = new Thread(this, "StreamEchoer")
  var lines = new Array[String](size)
  var pos = 0
  thread.start()

  def run() = {
    while(true) {
      val line = reader.readLine()
      println(line)
      lines(pos) = line
      pos = (pos + 1) % size
    }
  }

  def tail: String = {
    val startPos = pos
    (0 to size).flatMap(i => Option(lines((startPos + i) % size))).mkString("\n")
  }
}

class JavaExecutor extends Executor {
  val logger = Logger()
  val httpClient = new HttpClient()

  protected def loadClasspath(classSources: List[ClassSource]): String = classSources.map {
      case ServerSideJar(path) => path
      case S3CachedJar(urlString) => {
        val method = new GetMethod(urlString)
        httpClient.executeMethod(method)
        val instream = method.getResponseBodyAsStream
        val outfile = File.createTempFile("deploylibS3CachedJar", ".jar")
        val outstream = new FileOutputStream(outfile)

        var x = instream.read
        while(x != -1) {
          outstream.write(x)
          x = instream.read
        }
        instream.close
        outstream.close
        outfile.toString
      }
  }.mkString(":")

  override def launchTask(d: ExecutorDriver, taskDesc: TaskDescription): Unit = {
    logger.debug("Starting storage handler" + taskDesc.getTaskId())
    val tempDir = File.createTempFile("deploylib", "mesosJavaExecutorWorkingDir")
    tempDir.delete()
    tempDir.mkdir()

    val processDescription = JvmProcess(taskDesc.getArg())
    val classpath = loadClasspath(processDescription.classpath)
    logger.debug("Requested memory: " + taskDesc.getParams().get("mem"))
    val cmdLine = List[String]("/usr/bin/java",
                       "-server",
                       "-Xmx" + taskDesc.getParams().get("mem").toInt + "M",
                       "-Xms" + taskDesc.getParams().get("mem").toInt + "M",
                       "-XX:+HeapDumpOnOutOfMemoryError",
                       processDescription.props.map(kv => "-D%s=%s".format(kv._1, kv._2)).mkString(" "),
                       "-cp", classpath,
                       processDescription.mainclass) ++ processDescription.args

    logger.info("Execing: " + cmdLine.mkString(" "))
    d.sendStatusUpdate(new TaskStatus(taskDesc.getTaskId, TaskState.TASK_STARTING, new Array[Byte](0)))
    val proc = Runtime.getRuntime().exec(cmdLine.filter(_.size != 0).toArray, Array[String](), tempDir)
    val stdout = new StreamTailer(proc.getInputStream())
    val stderr = new StreamTailer(proc.getErrorStream())
    def output = List(cmdLine, processDescription, "===stdout===", stdout.tail,  "===stderr===", stderr.tail).mkString("\n").getBytes
    d.sendStatusUpdate(new TaskStatus(taskDesc.getTaskId, TaskState.TASK_RUNNING, output))
    val result = proc.waitFor()
    val finalTaskState = result match {
      case 0 => TaskState.TASK_FINISHED
      case _ => TaskState.TASK_FAILED
    }
    d.sendStatusUpdate(new TaskStatus(taskDesc.getTaskId, finalTaskState, output))
  }
}
