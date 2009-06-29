package deploylib

import scala.util.matching.Regex

class Service(id: String, instance: Instance) {
  
  def getId = id
  
  @throws(classOf[IllegalStateException])
  def start {
    checkService
    instance.exec("sv start /mnt/services/" + id)
  }
  
  /**
   * @param wait  How many times to check for a service's status.
   *              Checks are seperated by one second.
   * @return      Returns true if service started after given number of checks,
   *              false if service is still not in run state.
   */
  @throws(classOf[IllegalStateException])
  def blockingStart(wait: Int): Boolean = {
    checkService
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
  @throws(classOf[IllegalStateException])
  def blockingStart(): Boolean = {
    blockingStart(120)
  }
  
  @throws(classOf[IllegalStateException])
  def once {
    checkService
    instance.exec("sv once /mnt/services/" + id)
  }
  
  @throws(classOf[IllegalStateException])
  def stop {
    checkService
    instance.exec("sv stop /mnt/services/" + id)
  }
  
  @throws(classOf[IllegalStateException])
  def forceStop {
    checkService
    instance.exec("sv force-stop /mnt/services/" + id)
  }
  
  @throws(classOf[IllegalStateException])
  def status: ServiceStatus = {
    checkService
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
  
  @throws(classOf[IllegalStateException])
  def tailLog(): String = {
    checkService
    instance.exec("tail /mnt/services/" + id + "/log/current").getStdout
  }
  
  @throws(classOf[IllegalStateException])
  def running: Boolean = {
    status.getStatus() == "run"
  }
  
  private def checkService = {
    if (Util.responseError(instance.exec("ls /mnt/services/" + id))) {
      throw new IllegalStateException("Service not present.")
    }
  }
  
}
