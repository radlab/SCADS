package deploylib
package mesos

import java.net.URL

import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import net.lag.logging.Logger
import net.lag.configgy.Configgy

import ec2._
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import _root_.mesos._
import java.io.{ File, InputStream, BufferedReader, InputStreamReader, FileOutputStream }

import scala.collection.JavaConversions._
import scala.util.Random

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.{Server, Request};
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.jetty.handler.{ContextHandler, StatisticsHandler}
import org.mortbay.jetty.handler.AbstractHandler
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

object JavaExecutor {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("mesos")

    if (args.size != 1) {
      println("Usage: JavaExecutor <configFile>")
      System.exit(1)
    }

    Configgy.configure(args(0))
    val driver = new MesosExecutorDriver(new JavaExecutor())
    driver.run()
  }
}

class JavaExecutor extends Executor {
  val config = Configgy.config
  val logger = Logger()
  val httpClient = new HttpClient()

  val baseDir = new File(config("javaexecutor.workdir", "/tmp"))
  if (!baseDir.exists) baseDir.mkdir()

  val jarCache = new File(config("javaexecutor.jarcache", "/tmp/jarCache"))
  if (!jarCache.exists) jarCache.mkdir

  /* Keep a handle to all tasks that are running so we can kill it if needed later */
  val runningTasks = new scala.collection.mutable.HashMap[Int, RunningTask]

  abstract class RunningTask {
    def kill: Unit
  }

  class JettyApp(val taskId: Int, val warFile: File, driver: ExecutorDriver) extends RunningTask {
    val server = new Server()
    val connector = new SelectChannelConnector()
    connector.setPort(Integer.getInteger("jetty.port", 8080).intValue())
    server.setConnectors(Array[Connector](connector))
    val pool = new org.mortbay.thread.QueuedThreadPool
    logger.warning("Setting max thread pool size")
    pool.setMaxThreads(10)
    server.setThreadPool(pool)
    logger.warning("max pool size: %d",server.getThreadPool match {case p:org.mortbay.thread.QueuedThreadPool => p.getMaxThreads; case _ => -1})

    /* Create context for webapp and wrap it with stats handler */
    val statsWebApp = new StatisticsHandler()
    val webapp = new WebAppContext()
    webapp.setContextPath("/")
    webapp.setWar(warFile.getCanonicalPath)
    statsWebApp.addHandler(webapp)

    @volatile var running = true
    @volatile var requestsPerSec = 0.0
    val statsThread = new Thread("StatsThread") {
      var lastTime = System.currentTimeMillis()
      var lastCount = 0
      override def run(): Unit = {
        while(running) {
          Thread.sleep(5000)
          val currentTime = System.currentTimeMillis()
          val currentCount = statsWebApp.getRequests()
          requestsPerSec = (currentCount - lastCount) / ((currentTime - lastTime) / 1000)
          logger.debug("Updating statistics at %d %d: %f", currentTime, currentCount, requestsPerSec)
          lastTime = currentTime
          lastCount = currentCount
        }
      }
    }
    statsThread.start()

    /* Create a special context that reports /stats over http */
    val statsHandler = new AbstractHandler() {
      def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, dispatch: Int): Unit = {
        response.setContentType("text/html")
        response.setStatus(HttpServletResponse.SC_OK)
        response.getWriter().println(<status><RequestRate>{requestsPerSec}</RequestRate><RequestsTotal>{statsWebApp.getRequests()}</RequestsTotal></status>.toString)
        request.asInstanceOf[Request].setHandled(true)
      }
    }
    val statsContext = new ContextHandler()
    statsContext.setContextPath("/stats")
    statsContext.addHandler(statsHandler)

    /* add both the webapp and the stats context to the running server */
    server.setHandlers(Array(statsContext, statsWebApp))

    server.start()

    while(!server.isRunning()) {
      logger.info("Waiting for server to report isRunning == true")
      Thread.sleep(1000)
    }
    driver.sendStatusUpdate(new TaskStatus(taskId, TaskState.TASK_RUNNING, new Array[Byte](0)))

    def kill = {
      running = false
      server.stop()
    }
  }

  class ForkedJvm(val taskId: Int, val heapSize: Int, val classpath: String, val mainClass: String, val args: Seq[String], val properties: Map[String, String], driver: ExecutorDriver) extends RunningTask with Runnable {
     val logger = Logger()
    logger.debug("Requested memory: " + heapSize)
    val cmdLine = List[String]("/usr/bin/java",
      "-server",
      "-Xmx" + heapSize + "M",
      "-Xms" + heapSize + "M",
      "-XX:+HeapDumpOnOutOfMemoryError",
      "-XX:+UseConcMarkSweepGC",
      "-Djava.library.path=" + new File(System.getenv("MESOS_HOME"), "lib/java"),
      properties.map(kv => "-D%s=%s".format(kv._1, kv._2)).mkString(" "),
      "-cp", classpath,
      mainClass) ++ args

    logger.info("Execing: " + cmdLine.mkString(" "))
    val tempDir = newTempDir()
    val proc = Runtime.getRuntime().exec(cmdLine.filter(_.size != 0).toArray, Array[String](), tempDir)
    val stdout = new StreamTailer(proc.getInputStream())
    val stderr = new StreamTailer(proc.getErrorStream())
    def output = List(cmdLine, this, "===stdout===", stdout.tail, "===stderr===", stderr.tail).mkString("\n").getBytes
    val taskThread = new Thread(this, "Task " + taskId + "Monitor")
    taskThread.run()

    def run() = {
      driver.sendStatusUpdate(new TaskStatus(taskId, TaskState.TASK_RUNNING, output))

      val result = proc.waitFor()
      val finalTaskState = result match {
        case 0 => TaskState.TASK_FINISHED
        case _ => TaskState.TASK_FAILED
      }
      driver.sendStatusUpdate(new TaskStatus(taskId, finalTaskState, output))
      logger.info("Cleaning up working directory %s for %d", tempDir, taskId)
      deleteRecursive(tempDir)
      logger.info("Done cleaning up after Task %d", taskId)
    }
    def kill = proc.destroy()
  }

  def resolveClassSource(classSource: ClassSource): File = classSource match {
    case ServerSideJar(path) => new File(path)
    case S3CachedJar(urlString) => {
      val jarUrl = new URL(urlString)

      //Note: this makes the assumption that the name of the file is the Md5 hash of the file.
      var jarMd5 = new File(jarUrl.getFile).getName
      val cachedJar = new File(jarCache, jarMd5)

      //TODO: Locks incase there are multiple executors on a machine
      if ((!cachedJar.exists) || !(Util.md5(cachedJar) equals jarMd5)) {
        val method = new GetMethod(urlString)
        logger.info("Downloading %s", urlString)
        httpClient.executeMethod(method)
        val instream = method.getResponseBodyAsStream
        val outstream = new FileOutputStream(cachedJar)

        var x = instream.read
        while (x != -1) {
          outstream.write(x)
          x = instream.read
        }
        instream.close
        outstream.close
        logger.info("Download of %s complete", urlString)
      }
      cachedJar
    }
  }

  protected def loadClasspath(classSources: Seq[ClassSource]): String = classSources.map(resolveClassSource).map(_.getCanonicalPath).mkString(":")

  protected def newTempDir(): File = {
    val tempDir = File.createTempFile("workdir", "", baseDir)
    tempDir.delete()
    tempDir.mkdir()
    tempDir
  }

  override def launchTask(d: ExecutorDriver, taskDesc: TaskDescription): Unit = {
    val taskId = taskDesc.getTaskId //Note: use this because you can't hold on to taskDesc after this function exits.
    d.sendStatusUpdate(new TaskStatus(taskId, TaskState.TASK_STARTING, new Array[Byte](0)))

    val launchDelay = Random.nextInt(10 * 1000)
    logger.info("Delaying startup %dms to avoid overloading zookeeper", launchDelay)
    Thread.sleep(launchDelay)

    logger.info("Starting task" + taskId)
    val runningTask = JvmTask(taskDesc.getArg()) match {
      case JvmMainTask(classpath, mainclass, args, props) => new ForkedJvm(taskId, taskDesc.getParams().get("mem").toInt, loadClasspath(classpath), mainclass, args, props, d)
      case JvmWebAppTask(warFile) => new JettyApp(taskId, resolveClassSource(warFile), d)
    }

    runningTasks += ((taskId, runningTask))
    logger.info("Task %d started", taskId)
  }

  override def killTask(d: ExecutorDriver, taskId: Int): Unit = {
    runningTasks.get(taskId) match {
      case Some(runningTask) => {
        logger.info("Killing task %d", taskId)
        runningTask.kill
      }
      case None => logger.warning("Asked to kill nonexistant task %d", taskId)
    }

  }

  protected def deleteRecursive(f: File): Unit = {
    if (f.isDirectory)
      f.listFiles.foreach(deleteRecursive)
    f.delete()
  }
}
