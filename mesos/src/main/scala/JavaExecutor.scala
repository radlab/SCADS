package edu.berkeley.cs.scads.mesos

import edu.berkeley.cs.scads.comm._

import org.apache.log4j.Logger
import java.io.{File, InputStream, BufferedReader, InputStreamReader}
import mesos._

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
  val logger = Logger.getLogger("scads.javaexecutor")
  System.loadLibrary("mesos")

  override def launchTask(d: ExecutorDriver, taskDesc: TaskDescription): Unit = {
    logger.debug("Starting storage handler" + taskDesc.getTaskId())
    val tempDir = File.createTempFile("scads", "mesosJavaExecutor")
    tempDir.delete()
    tempDir.mkdir()

    val processDescription = new JvmProcess().parse(taskDesc.getArg())
    logger.debug("Requested memory: " + taskDesc.getParams().get("mem"))
    val cmdLine = List("/usr/lib/jvm/java-6-sun/bin/java",
                       "-server",
                       "-Xmx" + taskDesc.getParams().get("mem").toInt + "M",
                       "-cp", processDescription.classpath,
                       processDescription.mainclass) ++ processDescription.args

    logger.info("Execing: " + cmdLine.mkString(" "))
    val proc = Runtime.getRuntime().exec(cmdLine.toArray, Array[String](), tempDir)
    val stdout = new StreamTailer(proc.getInputStream())
    val stderr = new StreamTailer(proc.getErrorStream())
    val result = proc.waitFor()
    if(result == 0)
      d.sendStatusUpdate(new TaskStatus(taskDesc.getTaskId, TaskState.TASK_FINISHED, "".getBytes))
    else
      d.sendStatusUpdate(new TaskStatus(taskDesc.getTaskId, TaskState.TASK_FAILED, stdout.tail.getBytes))
  }
}
