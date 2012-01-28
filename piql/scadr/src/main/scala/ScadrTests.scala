package edu.berkeley.cs
package scads
package piql
package scadr

import storage.TestScalaEngine

object TestScadr {
  def main(args: Array[String]): Unit = {
    val client = new ScadrClient(TestScalaEngine.newScadsCluster(), new ParallelExecutor)

    val user = new User("marmbrus")
    user.homeTown = "Berkeley"
    client.users.put(user)

    println(client.findUser("marmbrus"))
    System.exit(0)
  }
}
