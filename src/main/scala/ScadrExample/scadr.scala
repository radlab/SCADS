package ScadrExample

import deploylib._
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

object Scadr {
  def run = {
    
    def runInstances(count: Int, typeString: String): InstanceGroup = {
      val imageId = InstanceType.bits(typeString) match {
        case 32 => "ami-e7a2448e"
        case 64 => "ami-e4a2448d"
      }
      val keyName = "abeitch"
      val keyPath = "/Users/aaron/.ec2/id_rsa-abeitch"
      val location = "us-east-1a"
      
      DataCenter.runInstances(imageId, count, keyName, keyPath, typeString, location)
    }
    
    println("Requesting instances.")
    val instances = runInstances(2, "m1.small")
    println("Instances received.")
    
    println("Waiting on instances to be ready.")
    instances.waitUntilReady
    println("Instances ready.")
    
    instances.get(0).tagWith("scads")
    instances.get(1).tagWith("loadbalancer")
    instances.get(1).tagWith("webserver")
    instances.get(1).tagWith("frontend")
    
    val scads = DataCenter.getInstanceGroupByTag("scads")
    val frontend = DataCenter.getInstanceGroupByTag("frontend").get(0)
    
    val scadsConfig = new JSONObject()
    val scadsRecipes = new JSONArray()
    scadsRecipes.put("scads::storage_engine")
    scadsRecipes.put("scads::scadr")
    scadsConfig.put("recipes", scadsRecipes)
    
    println("Deploying scads.")
    val scadsDeployment = scads.deployNonBlocking(scadsConfig)
    
    val frontendConfig = new JSONObject()
    val frontendRecipes = new JSONArray()
    frontendRecipes.put("cloudstone::haproxy")
    frontendRecipes.put("cloudstone::nginx")
    frontendConfig.put("recipes", frontendRecipes)

    val haproxyConfig = new JSONObject
    val haproxyServers = new JSONObject()
    scads.foreach(instance => {
      val server = new JSONObject()
      server.put("start", 8080)
      server.put("count", 1)
      haproxyServers.put(instance.privateDnsName, server)
    })
    
    haproxyConfig.put("servers", haproxyServers)
    frontendConfig.put("haproxy", haproxyConfig)
    
    val nginxConfig = new JSONObject()
    
    val nginxServers = new JSONObject()
    val haproxyServer = new JSONObject()
    haproxyServer.put("start", 4000)
    haproxyServer.put("count", 1)
    nginxServers.put("localhost", haproxyServer)
    
    nginxConfig.put("servers", nginxServers)
    frontendConfig.put("nginx", nginxConfig)
    
    println("Deploying frontend.")
    val frontendDeployment = frontend.deployNonBlocking(scadsConfig)
    
    scadsDeployment()
    println("Done deploying scads.")
    frontendDeployment()
    println("Done deploying frontend.")
    
    println("Point your browser to " + frontend.publicDnsName)
    
  }
}