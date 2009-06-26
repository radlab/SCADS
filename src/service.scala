package deploylib

class Service(id: String, instance: Instance) {
  
  def getId = id
  
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
  def blockingStart(wait: Int): Boolean = {
    checkService
    instance.exec("sv start /mnt/services/" + id)
    for (i <- 0 to wait) {
      if (status.getStatus() == "run") return true
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
  
  def once {
    checkService
    instance.exec("sv once /mnt/services/" + id)
  }
  
  def stop {
    checkService
    instance.exec("sv stop /mnt/services/" + id)
  }
  
  def forceStop {
    checkService
    instance.exec("sv force-stop /mnt/services/" + id)
  }
  
  def status: ServiceStatus = {
    checkService
    new ServiceStatus("", "", 0, 0)
  }
  
  def tailLog(): String = {
    checkService
    instance.exec("tail /mnt/services/" + id + "/log/current").getStdout
  }
  
  def checkService = {
    if (instance.exec("ls /mnt/services/" + id).getExitStatus != 0) {
      throw new IllegalStateException("Service not present.")
    }
  }
  
}
