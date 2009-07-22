package ScadrExample

import deploylib._
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

object Scadr {
  def run = {
    
    def runInstances(count: Int, typeString: String): InstanceGroup = {
      DataCenter.runInstances(count, typeString, true)
    }
    
    println("Requesting instances and waiting until they are ready.")
    val instances = runInstances(2, "m1.small")
    println("Instances received and ready.")
    
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
    
    println("Deploying scads and scadr.")
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
    val frontendDeployment = frontend.deployNonBlocking(frontendConfig)
    
    scadsDeployment()
    println("Done deploying scads and scadr.")
    frontendDeployment.value
    println("Done deploying frontend.")
    
    println("Point your browser to " + frontend.publicDnsName)
    
  }
}