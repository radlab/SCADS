package edu.berkeley.cs
package scads
package piql
package scadr

import storage._
import scadr._

import org.apache.avro.util.Utf8

object TestScadr {
  def main(args: Array[String]): Unit = {
    val client = new ScadrClient(TestScalaEngine.getTestCluster, new SimpleExecutor)
    //Insert Data
    val user = User("marmbrus")
    user.homeTown = "berkeley"
    client.users.put(user.key, user.value)

    //Run Simple Query
    println(client.findUser(new Utf8("marmbrus")  :: Nil))
    System.exit(0)
  }
}
