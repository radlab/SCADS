package edu.berkeley.cs
package radlab
package demo

import collection.JavaConversions._

import deploylib.ec2._
import scads.comm._

import com.amazonaws.services.elasticloadbalancing._
import com.amazonaws.services.elasticloadbalancing.model._

import net.lag.logging.Logger

object LoadBalancer extends AWSConnection {
  import DemoConfig._
  val logger = Logger()
  val client = new AmazonElasticLoadBalancingClient(credentials)

  def createLoadBalancer(name: String): String  = {
    val listeners = new Listener("HTTP", 80, 8080) :: Nil
    val request = new CreateLoadBalancerRequest(name, listeners, zone :: Nil)
    val response = client.createLoadBalancer(request)
    response.getDNSName()
  }

  def update(name: String, servers: ZooKeeperProxy#ZooKeeperNode): Unit = {
    val internalAddresses = new String(servers.data).split("\n")
    val activeInstances = internalAddresses.flatMap(addr => EC2Instance.activeInstances.find(_.privateDnsName equals addr)).map(_.instanceId)
    logger.info("Currently active instances for app %s: %s", name, activeInstances.mkString(","))

    val getCurrentInstancesResponse = client.describeLoadBalancers(new DescribeLoadBalancersRequest(name :: Nil))
    val currentInstances = getCurrentInstancesResponse.getLoadBalancerDescriptions.head.getInstances.map(_.getInstanceId)
    val toRegister = activeInstances.filterNot(currentInstances.contains)
    val toRemove = currentInstances.filterNot(activeInstances.contains)

    if(toRegister.size > 0) {
      logger.info("Adding servers to loadbalancer for app %s: %s", name, toRegister.mkString(","))
      val registerRequest = new RegisterInstancesWithLoadBalancerRequest(name, 
									 toRegister.map(new Instance(_)).toList)
      client.registerInstancesWithLoadBalancer(registerRequest)
    }

    if(toRemove.size > 0) {
      logger.info("Removing servers from loadbalancer for app %s: %s", name, toRemove.mkString(","))
      val deregisterRequest = new DeregisterInstancesFromLoadBalancerRequest(name,
									     toRemove.map(new Instance(_)).toList)
      client.deregisterInstancesFromLoadBalancer(deregisterRequest)
    }
  }
}
