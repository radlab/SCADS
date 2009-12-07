package deploylib.chef

import deploylib._

case class HAProxyService(remoteMachine: RemoteMachine,
                          config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "rails"

  remoteMachine.addService(this)

  def addRails: Unit = {
    // TODO: Implement me.
  }

}
