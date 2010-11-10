package deploylib
package mesos

import java.io.{File, InputStream, BufferedReader, InputStreamReader, FileOutputStream}
import java.net.URL

import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import net.lag.logging.Logger
import net.lag.configgy.Configgy

import ec2._
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import _root_.mesos._

import scala.collection.JavaConversions._
import scala.util.Random

object JavaExecutor {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("mesos")

    if(args.size != 1) {
      println("Usage: JavaExecutor <configFile>")
      System.exit(1)
    }

    Configgy.configure(args(0))
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
    var line = reader.readLine()
    while(line != null) {
      println(line)
      lines(pos) = line
      pos = (pos + 1) % size
      line = reader.readLine()
    }
  }

  def tail: String = {
    val startPos = pos
    (0 to size).flatMap(i => Option(lines((startPos + i) % size))).mkString("\n")
  }
}

class JavaExecutor extends Executor {
  val config = Configgy.config
  val logger = Logger()
  val httpClient = new HttpClient()

  val baseDir = new File(config("javaexecutor.workdir", "/tmp"))
  if(!baseDir.exists) baseDir.mkdir()

  val jarCache = new File(config("javaexecutor.jarcache", "/tmp/jarCache"))
  if(!jarCache.exists) jarCache.mkdir

  /* Keep a handle to all tasks that are running so we can kill it if needed later */
  case class RunningTask(proc: Process, stdout: StreamTailer, stderr: StreamTailer)
  val runningTasks = new scala.collection.mutable.HashMap[Int, RunningTask]

  protected def loadClasspath(classSources: Seq[ClassSource]): String = classSources.map {
      case ServerSideJar(path) => path
      case S3CachedJar(urlString) => {
        val jarUrl = new URL(urlString)

        //Note: this makes the assumption that the name of the file is the Md5 hash of the file.
        var jarMd5 = new File(jarUrl.getFile).getName
        val cachedJar = new File(jarCache, jarMd5)

        //TODO: Locks incase there are multiple executors on a machine
        if((!cachedJar.exists) || !(Util.md5(cachedJar) equals jarMd5)) {
          val method = new GetMethod(urlString)
          logger.info("Downloading %s", urlString)
          httpClient.executeMethod(method)
          val instream = method.getResponseBodyAsStream
          val outstream = new FileOutputStream(cachedJar)

          var x = instream.read
          while(x != -1) {
            outstream.write(x)
            x = instream.read
          }
          instream.close
          outstream.close
          logger.info("Download of %s complete", urlString)
        }
        cachedJar.getCanonicalPath()
      }
  }.mkString(":")

  override def launchTask(d: ExecutorDriver, taskDesc: TaskDescription): Unit = {
    val taskId = taskDesc.getTaskId //Note: use this because you can't hold on to taskDesc after this function exits.
    d.sendStatusUpdate(new TaskStatus(taskId, TaskState.TASK_STARTING, new Array[Byte](0)))

    val launchDelay = Random.nextInt(10*1000)
    logger.info("Delaying startup %dms to avoid overloading zookeeper", launchDelay)
    Thread.sleep(launchDelay)


    val tempDir = File.createTempFile("workdir", "", baseDir)
    tempDir.delete()
    tempDir.mkdir()

    logger.info("Starting task" + taskId)
    val processDescription = JvmProcess(taskDesc.getArg())
    val classpath = loadClasspath(processDescription.classpath)
    logger.debug("Requested memory: " + taskDesc.getParams().get("mem"))
    val cmdLine = List[String]("/usr/bin/java",
                       "-server",
                       "-Xmx" + taskDesc.getParams().get("mem").toInt + "M",
                       "-Xms" + taskDesc.getParams().get("mem").toInt + "M",
                       "-XX:+HeapDumpOnOutOfMemoryError",
                       "-XX:+UseConcMarkSweepGC",
                       processDescription.props.map(kv => "-D%s=%s".format(kv._1, kv._2)).mkString(" "),
                       "-cp", classpath,
                       processDescription.mainclass) ++ processDescription.args

    logger.info("Execing: " + cmdLine.mkString(" "))
    val proc = Runtime.getRuntime().exec(cmdLine.filter(_.size != 0).toArray, Array[String](), tempDir)
    val stdout = new StreamTailer(proc.getInputStream())
    val stderr = new StreamTailer(proc.getErrorStream())
    def output = List(cmdLine, processDescription, "===stdout===", stdout.tail,  "===stderr===", stderr.tail).mkString("\n").getBytes

    val taskThread = new Thread("Task " + taskId + "Monitor") {
      override def run() = {
        val result = proc.waitFor()
        val finalTaskState = result match {
          case 0 => TaskState.TASK_FINISHED
          case _ => TaskState.TASK_FAILED
        }
        d.sendStatusUpdate(new TaskStatus(taskId, finalTaskState, output))
        logger.info("Cleaning up working directory %s for %d", tempDir, taskId)
        deleteRecursive(tempDir)
        logger.info("Done cleaning up after Task %d", taskId)
      }
    }
    taskThread.start()
    runningTasks += ((taskId, RunningTask(proc, stdout, stderr)))
    d.sendStatusUpdate(new TaskStatus(taskId, TaskState.TASK_RUNNING, output))
    logger.info("Task %d started", taskId)
  }

  override def killTask(d: ExecutorDriver, taskId: Int): Unit = {
    runningTasks.get(taskId) match {
      case Some(runningTask) => {
        logger.info("Killing task %d", taskId)
        runningTask.proc.destroy()
      }
      case None => logger.warning("Asked to kill nonexistant task %d", taskId)
    }

  }

  protected def deleteRecursive(f: File): Unit = {
    if(f.isDirectory)
      f.listFiles.foreach(deleteRecursive)
    f.delete()
  }
}
