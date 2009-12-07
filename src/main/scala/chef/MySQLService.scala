package deploylib.chef

import deploylib._

/*************************
{
    "mysql": {
        "port": 3306,
        "server_id": 1
    },
    "recipes": ["cloudstone::mysql"]
}
*************************/

case class MySQLService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "mysql"

  remoteMachine.addService(this)

  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    service match {
      case _ =>
        // TODO: Throw an exception for unhandled dependency.
    }
  }

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
  }

}
