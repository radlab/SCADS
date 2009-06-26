package deploylib

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
      if (status.getStatus() == "run") return true
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
    new ServiceStatus("", "", 0, 0)
  }
  
  @throws(classOf[IllegalStateException])
  def tailLog(): String = {
    checkService
    instance.exec("tail /mnt/services/" + id + "/log/current").getStdout
  }
  
  private def checkService = {
    if (instance.exec("ls /mnt/services/" + id).getExitStatus != 0) {
      throw new IllegalStateException("Service not present.")
    }
  }
  
}
