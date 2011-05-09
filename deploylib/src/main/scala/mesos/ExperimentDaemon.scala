package deploylib
package mesos

import edu.berkeley.cs._
import scads.comm._
import avro.marker._

import org.apache.mesos.Protos._

import net.lag.logging.Logger

case class RemoteServiceScheduler(var host: String, var port: Int, var id: ActorId) extends AvroRecord with ExperimentScheduler with RemoteActorProxy {
  def scheduleExperiment(processes: Seq[JvmTask]): Unit = {
    this !? (RunExperimentRequest(processes.toList), 60 * 1000)
  }

  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit = {
    logger.info("Received %s from %s", msg, src)
  }
}

class ServiceScheduler(mesosMaster: String, executor: String) extends LocalExperimentScheduler("Meta-Scheduler", mesosMaster, executor) with ServiceHandler[ExperimentOperation] {
  def startup: Unit = null
  def shutdown: Unit = null

  def process(src: Option[RemoteActorProxy], msg: ExperimentOperation) = msg match {
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
      logger.info("Killing task %d", taskId)
      driver.killTask(TaskID.newBuilder.setValue(taskId).build())
      src.foreach(_ ! KillTaskResponse())
    }
  }
}
