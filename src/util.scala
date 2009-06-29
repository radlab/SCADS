package DeployLib

object Util {
  def responseError(response: ExecuteResponse): Boolean = {
    val exitStatus = response.getExitStatus()
    if (exitStatus != null && != 0) return true
    if (exitStatus == null && response.getStderr.length() > 0) return true
    return false
  }
}