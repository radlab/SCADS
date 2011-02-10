package deploylib
package mesos

import edu.berkeley.cs.scads.comm._

import net.lag.logging.Logger

class RemoteServiceScheduler extends ExperimentScheduler with MessageReceiver {
  val logger = Logger()
  val remoteService = RemoteActor("mesos-ec2", 9001, ActorNumber(0))
  implicit val returnAddress = MessageHandler.registerService(this)

  def scheduleExperiment(processes: Seq[JvmTask]): Unit = {
    remoteService ! RunExperimentRequest(processes.toList)
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
      logger.info("Processing Requst to run %s", processes.map(taskDescription).mkString(","))
      scheduleExperiment(processes)
      src.foreach(_ ! RunExperimentResponse())
    }
    case KillTaskRequest(taskId) => {
      logger.info("Killing task %d", taskId)
      driver.killTask(taskId)
      src.foreach(_ ! KillTaskResponse())
    }
  }
}
