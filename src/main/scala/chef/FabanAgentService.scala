package deploylib.chef

import deploylib._

/*************************
{
    "faban": {
        "jdbc": "mysql"
    },
    "recipes": ["cloudstone::faban-agent"]
}
*************************/

case class FabanAgentService(remoteMachine: RemoteMachine,
                             config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "faban-agent"

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
