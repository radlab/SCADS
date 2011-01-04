package edu.berkeley.cs
package scads
package piql
package scadr

import storage.TestScalaEngine

object TestScadr {
  def main(args: Array[String]): Unit = {
    val client = new ScadrClient(TestScalaEngine.newScadsCluster(), new SimpleExecutor)

    val user = new User("marmbrus")
    user.homeTown = "Berkeley"
    client.users.put(user.key, user.value)

    println(client.findUser("marmbrus"))
    System.exit(0)
  }
}
