package edu.berkeley.cs
package radlab
package demo

import scads.piql._
import scads.piql.scadr._
import scads.storage._

import net.lag.logging.Logger

object DemoScadr {
  import DemoConfig._

  val logger = Logger()
  val cluster = new ScadsCluster(scadrRoot)
  val client = new ScadrClient(cluster, new SimpleExecutor, 10)
  
  logger.info("Creating demo user for scadr")
  val demoUser = new User("radlabDemo")
  demoUser.homeTown = "Berkeley, CA"
  demoUser.password = "feb24"
  client.users.put(demoUser.key, demoUser.value)

  def post(thoughtText: String): Unit = {
    val thought = new Thought(demoUser.username, (System.currentTimeMillis / 1000).toInt)
    thought.text = thoughtText
    logger.info("Thinking to scadr: %s", thought)
    client.thoughts.put(thought.key, thought.value)
  }
}
