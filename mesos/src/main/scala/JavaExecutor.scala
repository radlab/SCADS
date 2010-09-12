package edu.berkeley.cs.scads.mesos

import edu.berkeley.cs.scads.comm._

import net.lag.logging.Logger
import java.io.{File, InputStream, BufferedReader, InputStreamReader}
import com.googlecode.avro.marker._
import mesos._

case class JvmProcess(var classpath: String, var mainclass: String, var args: List[String], var props: Map[String, String] = Map.empty) extends AvroRecord

object JavaExecutor {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("mesos")
    org.apache.log4j.BasicConfigurator.configure()
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
  System.loadLibrary("mesos")

  override def launchTask(d: ExecutorDriver, taskDesc: TaskDescription): Unit = {
    logger.debug("Starting storage handler" + taskDesc.getTaskId())
    val tempDir = File.createTempFile("scads", "mesosJavaExecutor")
    tempDir.delete()
    tempDir.mkdir()

    val processDescription = classOf[JvmProcess].newInstance.parse(taskDesc.getArg())
    logger.debug("Requested memory: " + taskDesc.getParams().get("mem"))
    val cmdLine = List("/usr/lib/jvm/java-6-sun/bin/java",
                       "-server",
                       "-Xmx" + taskDesc.getParams().get("mem").toInt + "M",
                       processDescription.props.map(kv => "-D%s=%s".format(kv._1, kv._2)).mkString(" "),
                       "-cp", processDescription.classpath,
                       processDescription.mainclass) ++ processDescription.args

    logger.info("Execing: " + cmdLine.mkString(" "))
    d.sendStatusUpdate(new TaskStatus(taskDesc.getTaskId, TaskState.TASK_STARTING, new Array[Byte](0)))
    val proc = Runtime.getRuntime().exec(cmdLine.toArray, Array[String](), tempDir)
    val stdout = new StreamTailer(proc.getInputStream())
    val stderr = new StreamTailer(proc.getErrorStream())
    def output = List("===stdout===", stdout.tail,  "===stderr===", stderr.tail).mkString("\n").getBytes
    d.sendStatusUpdate(new TaskStatus(taskDesc.getTaskId, TaskState.TASK_RUNNING, output))
    val result = proc.waitFor()
    val finalTaskState = result match {
      case 0 => TaskState.TASK_FINISHED
      case _ => TaskState.TASK_FAILED
    }
    d.sendStatusUpdate(new TaskStatus(taskDesc.getTaskId, finalTaskState, output))
  }
}
