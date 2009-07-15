/**
 * An example use of the deployment library to deploy Cloudstone
 */
 
package ScalaCloudstoneExample
 
import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

object Cloudstone {
  def start = {
    /**
     * The stack will have the following defaults for the other roles:
     * 1 MySQL server on an c1.xlarge instance
     * 1 HAProxy server on a m1.small instance
     * 1 nginx server on a m1.small instance
     * 1 Faban master/driver server on a c1.xlarge
     */
 
 
    val railsSettings   = (2, "c1.xlarge", InstanceType.cores("c1.xlarge") * 2)

    val mysqlSettings   = (1, "c1.xlarge")
    val haproxySettings = (1, "m1.small")
    val nginxSettings   = (1, "m1.small")
    val fabanSettings   = (1, "c1.xlarge")


    /* A shortcut method for DataCenter.runInstances provided in API */
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


    val rails   = runInstances(railsSettings._1, railsSettings._2)
    val mysql   = runInstances(mysqlSettings._1, mysqlSettings._2)
    val haproxy = runInstances(haproxySettings._1, haproxySettings._2)
    val nginx   = runInstances(nginxSettings._1, nginxSettings._2)
    val faban   = runInstances(fabanSettings._1, fabanSettings._2)
    
    val allInstances = new InstanceGroup()
    allInstances ++ rails ++ mysql ++ haproxy ++ nginx ++ faban
    
    println("Waiting until all instances are ready.")
    allInstances.parallelMap((instance: Instance) => instance.waitUntilReady)
    
    println("Building configurations")
    val mysqlPort = 3306
    /* Rails Configuration */
    val railsConfig = new JSONObject()
    railsConfig.put("recipes", new JSONArray().put("cloudstone::rails"))
    val railsRails = new JSONObject()
    
    val railsRailsPorts = new JSONObject()
    railsRailsPorts.put("start", 3000)
    railsRailsPorts.put("count", railsSettings._3)
    railsRails.put("ports", railsRailsPorts)
    
    val railsRailsDatabase = new JSONObject()
    railsRailsDatabase.put("host", mysql.getFirst().privateDnsName)
    railsRailsDatabase.put("adapter", "mysql")
    railsRailsDatabase.put("port", mysqlPort)
    railsRails.put("database", railsRailsDatabase)
    
    val railsRailsMemcached = new JSONObject()
    railsRailsMemcached.put("host", "localhost")
    railsRailsMemcached.put("port", 1211)
    railsRails.put("memcached", railsRailsMemcached)
    
    val railsRailsGeocoder = new JSONObject()
    railsRailsGeocoder.put("host", faban.getFirst().privateDnsName)
    railsRailsGeocoder.put("port", 9980)
    railsRails.put("geocoder", railsRailsGeocoder)
    
    railsConfig.put("rails", railsRails)
    
    /* mysql configuration */
    val mysqlConfig = new JSONObject()
    val mysqlRecipes = new JSONArray()
    mysqlRecipes.put("cloudstone::mysql")
    mysqlRecipes.put("cloudstone::faban-agent")
    mysqlConfig.put("recipes", mysqlRecipes)
    val mysqlMysql = new JSONObject()
    
    mysqlMysql.put("server_id", 1)
    mysqlMysql.put("port", mysqlPort)
    mysqlConfig.put("mysql", mysqlMysql)
    
    val mysqlFabanAgent = new JSONObject()
    mysqlFabanAgent.put("jdbc", "mysql")
    mysqlConfig.put("faban", mysqlFabanAgent)
    
    /* haproxy configuration */
    val haproxyConfig = new JSONObject()
    haproxyConfig.put("recipes", new JSONArray().put("cloudstone::haproxy"))
    val haproxyHaproxy = new JSONObject()
    haproxyHaproxy.put("port", 4000)
    
    val haproxyHaproxyServers = new JSONObject()
    rails.foreach(instance => {
      val server = new JSONObject()
      server.put("start", 3000)
      server.put("count", railsSettings._3)
      haproxyHaproxyServers.put(instance.privateDnsName, server)
    })
    haproxyHaproxy.put("servers", haproxyHaproxyServers)
    
    haproxyConfig.put("haproxy", haproxyHaproxy)
    
    /* nginx configuration */
    val nginxConfig = new JSONObject()
    val nginxRecipes = new JSONArray()
    nginxRecipes.put("cloudstone::nginx")
    nginxRecipes.put("cloudstone::faban-agent")
    nginxConfig.put("recipes", nginxRecipes)
    val nginxNginx = new JSONObject()
    val nginxNginxServers = new JSONObject()
    
    haproxy.foreach(instance => {
      val server = new JSONObject()
      server.put("start", 4000)
      server.put("count", 1)
      nginxNginxServers.put(instance.privateDnsName, server)
    })
    nginxNginx.put("servers", nginxNginxServers)
    nginxConfig.put("nginx", nginxNginx)
    
    val nginxFaban = new JSONObject()
    nginxFaban.put("mysql", false)
    nginxFaban.put("postgresql", false)
    nginxConfig.put("faban", nginxFaban)
    
    /* faban configuration */
    val fabanConfig = new JSONObject()
    fabanConfig.put("recipes", new JSONArray().put("cloudstone::faban"))
    val fabanFaban = new JSONObject()
    val fabanFabanHosts = new JSONObject()
    fabanFabanHosts.put("driver", faban.getFirst().privateDnsName)
    fabanFabanHosts.put("webserver", nginx.getFirst().privateDnsName)
    fabanFabanHosts.put("database", mysql.getFirst().privateDnsName)
    fabanFabanHosts.put("storage", "")
    fabanFabanHosts.put("cache", "")
    
    val fabanFabanDatabase = new JSONObject()
    fabanFabanDatabase.put("adapter", "mysql")
    fabanFabanDatabase.put("port", mysqlPort)
    
    fabanFaban.put("hosts", fabanFabanHosts)
    fabanFaban.put("database", fabanFabanDatabase)
    fabanFaban.put("debug", false)
    fabanConfig.put("faban", fabanFaban)
    
    def deployMaker(jsonConfig: JSONObject): (Instance) => Unit = {
      (instance: Instance) => {
        instance.deploy(jsonConfig)
      }
    }
    
    println("Deploying mysql.")
    mysql.parallelMap(deployMaker(mysqlConfig))
    println("Deploying rails.")
    rails.parallelMap(deployMaker(railsConfig))
    println("Deploying haproxy.")
    haproxy.parallelMap(deployMaker(haproxyConfig))
    println("Deploying nginx.")
    nginx.parallelMap(deployMaker(nginxConfig))
    println("Deploying faban.")
    faban.parallelMap(deployMaker(fabanConfig))
    
    def startAllServices(instance: Instance): Unit = {
      def startService(service: Service): Unit = {service.start}
      instance.getAllServices.foreach(startService)
    }
    
    //allInstances.parallelMap(startAllServices)
    println("All done")
  }
}