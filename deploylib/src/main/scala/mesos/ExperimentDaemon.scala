package deploylib
package mesos

import edu.berkeley.cs._
import scads.comm._
import avro.marker._

import org.apache.mesos.Protos._

import net.lag.logging.Logger

case class RemoteServiceScheduler(var remoteNode: RemoteNode, var id: ServiceId) extends AvroRecord with RemoteServiceProxy[Message] with ExperimentScheduler  {
  registry = MsgHandler

  def scheduleExperiment(processes: Seq[JvmTask]): Unit = {
    this !? (RunExperimentRequest(processes.toList), 60 * 1000)
  }
}

class ServiceScheduler(mesosMaster: String, executor: String) extends LocalExperimentScheduler("Meta-Scheduler", mesosMaster, executor) with ServiceHandler[Message] {
  def startup: Unit = null
  def shutdown: Unit = null

  val registry = MsgHandler

  def process(src: Option[RemoteServiceProxy[Message]], msg: Message) = msg match {
    case RunExperimentRequest(processes) => {
      /*
      val processDescription = processes.map{
        case j:JvmMainTask => j.mainclass + " " + j.args
        case j:JvmWebAppTask => j.warFile
      }
      logger.info("Processing Requst to run %s", processDescription.mkString(","))
      */
      logger.info("Processing Requst from %s to run %s", src.getOrElse("UnknownActor"), processes.map(taskDescription).mkString(","))
      scheduleExperiment(processes)
      src.foreach(_ ! RunExperimentResponse())
    }
    case KillTaskRequest(taskId) => {
      logger.info("Killing task %s", taskId)
      driver.killTask(TaskID.newBuilder.setValue(taskId).build())
      src.foreach(_ ! KillTaskResponse())
    }
    case otherMsg => logger.error("Received unexpected message: %s", otherMsg)
  }
}
