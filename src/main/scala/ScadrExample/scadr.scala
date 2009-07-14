package ScadrExample

import deploylib._
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

object Scadr {
  def run = {
    
    def runInstances(count: Int, typeString: String): InstanceGroup = {
      val imageId = InstanceType.bits(typeString) match {
        case "32-bit" => "ami-e7a2448e"
        case "64-bit" => "ami-e4a2448d"
      }
      val keyName = "abeitch"
      val keyPath = "/Users/aaron/.ec2/id_rsa-abeitch"
      val location = "us-east-1a"
      
      DataCenter.runInstances(imageId, count, keyName, keyPath, typeString, location)
    }
    
    println("Requesting instances.")
    val instances = runInstances(3, "m1.small")
    println("Instances received.")
    
    println("Waiting on instances to be ready.")
    instances.parallelMap((instance) => instance.waitUntilReady)
    println("Instances ready.")
    
    instances.get(0).tagWith("scads")
    instances.get(1).tagWith("loadbalancer")
    instances.get(2).tagWith("webserver")
    
    val scads = DataCenter.getInstanceGroupByTag("scads")
    val loadbalancer = DataCenter.getInstanceGroupByTag("loadbalancer").get(0)
    val webserver = DataCenter.getInstanceGroupByTag("webserver").get(0)
    
    val scadsConfig = new JSONObject()
    val scadsRecipes = new JSONArray()
    scadsRecipes.put("scads::storage_engine")
    scadsRecipes.put("scads::scadr")
    scadsConfig.put("recipes", scadsRecipes)
    
    println("Deploying scads.")
    scads.get(0).deploy(scadsConfig)
    println("Done deploying scads.")
    
    val haproxyConfig = new JSONObject()
    val haproxyRecipes = new JSONArray()
    haproxyRecipes.put("cloudstone::haproxy")
    haproxyConfig.put("recipes", haproxyRecipes)
    
    val haproxyHaproxy = new JSONObject
    val haproxyServers = new JSONObject()
    scads.foreach(instance => {
      val server = new JSONObject()
      server.put("start", 8080)
      server.put("count", 1)
      haproxyServers.put(instance.privateDnsName, server)
    })
    
    haproxyHaproxy.put("servers", haproxyServers)
    
    haproxyConfig.put("haproxy", haproxyHaproxy)
    
    println("Deploying haproxy.")
    loadbalancer.deploy(haproxyConfig)
    println("Done deploying haproxy.")
    
    val nginxConfig = new JSONObject()
    val nginxRecipes = new JSONArray()
    nginxRecipes.put("cloudstone::nginx")
    nginxConfig.put("recipes", nginxRecipes)
    
    val nginxNginx = new JSONObject()
    val nginxServers = new JSONObject()
    val haproxyServer = new JSONObject()
    haproxyServer.put("start", 4000)
    haproxyServer.put("count", 1)
    nginxServers.put(loadbalancer.privateDnsName, haproxyServer)
    
    nginxNginx.put("servers", nginxServers)
    nginxConfig.put("nginx", nginxNginx)
    
    println("Deploying nginx.")
    webserver.deploy(nginxConfig)
    println("Done deploying nginx.")
    
    println("Point your browser to " + webserver.publicDnsName)
    
  }
}