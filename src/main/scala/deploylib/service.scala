package deploylib

import scala.util.matching.Regex

class Service(id: String, instance: Instance) {
  checkService
  
  /**
   * Returns id/name of service.
   */
  def getId = id
  
  /**
   * Tells runit to start the service.
   */
  def start {
    instance.exec("sv start /mnt/services/" + id)
  }
  
  /**
   * Tells runit to start the service and checks the service status the given
   * number of times with 1 second sleeps in between. Stops when the service
   * is in the running state.
   *
   * @param wait  How many times to check for a service's status.
   *              Checks are seperated by one second.
   * @return      Returns true if service started after given number of checks,
   *              false if service is still not in run state.
   */
  def blockingStart(wait: Int): Boolean = {
    instance.exec("sv start /mnt/services/" + id)
    for (i <- 0 to wait) {
      if (running) return true
      Thread.sleep(1000)
    }
    return false
  }
  
  /**
   * blockingStart(120)
   */
  def blockingStart(): Boolean = {
    blockingStart(120)
  }
  
  /**
   * Tells runit to 'once' the service, which means to start the service once
   * and don't try to restart it if it dies.
   */
  def once {
    instance.exec("sv once /mnt/services/" + id)
  }
  
  /**
   * Tells runit to stop the service.
   */
  def stop {
    instance.exec("sv stop /mnt/services/" + id)
  }
  
  /**
   * Runit tries very hard to stop the service if it won't stop through, the
   * normal means.
   */
  def forceStop {
    instance.exec("sv force-stop /mnt/services/" + id)
  }
  
  /**
   * Gets the output of running a runit status command and wraps it in a
   * ServiceStatus object.
   */
  def status: ServiceStatus = {
    val RunningRegex = new Regex(
          """(\S+): /mnt/services/(\S+): \(pid (\d+)\) (\d+)s.*""")
    val StoppedRegex = new Regex(
          """(down): /mnt/services/(\S+): (\d+)s,.*""")
      
    val statusResponse = instance.exec("sv status /mnt/services/" + id)
          
    statusResponse.getStdout match {
      case RunningRegex(state, id, pid, uptime) =>
                        new ServiceStatus(state, id, pid.toInt, uptime.toInt)
      case StoppedRegex(state, id, uptime)      =>
                        new ServiceStatus(state, id, -1, uptime.toInt)
      case _ => throw new IllegalStateException("Status method messed up:\n" + 
                                                statusResponse.toString())
    }
  }
  
  /**
   * Gets the output of a tail on the service's log. Uses the default tail
   * length: the last 10 lines.
   */
  def tailLog(): String = {
    instance.exec("tail /mnt/services/" + id + "/log/current").getStdout
  }
  
  /**
   * Gets the output of a tail on the service's log.
   *
   * @param count The number of lines you want.
   */
  def tailLog(count: Int): String = {
    instance.exec("tail -n " + count + " /mnt/services/" + id + "/log/current").getStdout
  }
  
  /**
   * Returns true if the status of the service is 'run'.
   */
  def running: Boolean = {
    status.getStatus() == "run"
  }
  
  private def checkService = {
    if (instance.exec("ls /mnt/services/" + id).error) {
      throw new IllegalStateException("Service not present.")
    }
  }
  
}
