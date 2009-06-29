/**
 * An example use of the deployment library to deploy Cloudstone
 */
 
import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import java.util.HashMap


object Cloudstone {
  def main(args: Array[String]) = {
    /**
     * This is a simple example where you pass on the command line
     * the number and size of rails servers you would like in the 
     * following form:
     *
     * scala cloudstone --count 2 --type c1.xlarge
     *
     * The stack will have the following defaults for the other roles:
     * 1 MySQL server on an c1.xlarge instance
     * 1 HAProxy server on a m1.small instance
     * 1 nginx server on a m1.small instance
     * 1 Faban master/driver server on a c1.xlarge
     */
 
 
    val railsSettings   = (args(1).toInt, args(3), Instance.cores(Instance.Type.valueOf(args(3)).get) * 2)

    val mysqlSettings   = (1, "c1.xlarge")
    val haproxySettings = (1, "m1.small")
    val nginxSettings   = (1, "m1.small")
    val fabanSettings   = (1, "c1.xlarge")


    /* A shortcut method for DataCenter.runInstances provided in API */
    def runInstances(count: Int, type_string: String): InstanceGroup = {
      val instance_type = Instance.Type.valueOf(type_string).get
      val imageId = Instance.bits(instance_type) match {
        case "32-bit" => "ami-e7a2448e"
        case "64-bit" => "ami-e4a2448d"
      }
      val keyName = "abeitch"
      val keyPath = "/Users/aaron/.ec2/id_rsa-abeitch"
      val location = "us-east-1a"
      
      DataCenter.runInstances(imageId, count, keyName, keyPath, instance_type, location)
    }


    val rails   = runInstances(railsSettings._1, railsSettings._2)
    val mysql   = runInstances(mysqlSettings._1, mysqlSettings._2)
    val haproxy = runInstances(haproxySettings._1, haproxySettings._2)
    val nginx   = runInstances(nginxSettings._1, nginxSettings._2)
    val faban   = runInstances(fabanSettings._1, fabanSettings._2)
    
    val allInstances = new InstanceGroup(rails.getList ++ mysql.getList ++ 
              haproxy.getList ++ nginx.getList ++ faban.getList)
    
    allInstances.parallelExecute((instance: Instance) => instance.waitUntilReady)
    
    /* Rails Configuration */
    val railsConfig = new JSONObject()
    railsConfig.put("recipes", new JSONArray().put("cloudstone::rails"))
    val railsRails = new JSONObject()
    
    val railsRailsPorts = new JSONObject()
    railsRailsPorts.put("start", 3000)
    railsRailsPorts.put("count", railsSettings._3)
    railsRails.put("ports", railsRailsPorts)
    
    val railsRailsDatabase = new JSONObject()
    railsRailsDatabase.put("host", mysql.getList.head.privateDnsName)
    railsRailsDatabase.put("adapter", "mysql")
    railsRails.put("database", railsRailsDatabase)
    
    val railsRailsMemcached = new JSONObject()
    railsRailsMemcached.put("host", "localhost")
    railsRailsMemcached.put("port", 1211)
    railsRails.put("memcached", railsRailsMemcached)
    
    val railsRailsGeocoder = new JSONObject()
    railsRailsGeocoder.put("host", faban.getList.head.privateDnsName)
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
    mysqlConfig.put("mysql", mysqlMysql)
    
    val mysqlFaban = new JSONObject()
    mysqlFaban.put("mysql", true)
    mysqlConfig.put("faban", mysqlFaban)
    
    /* haproxy configuration */
    val haproxyConfig = new JSONObject()
    haproxyConfig.put("recipes", new JSONArray().put("cloudstone::haproxy"))
    val haproxyHaproxy = new JSONObject()
    haproxyHaproxy.put("port", 4000)
    
    val haproxyHaproxyServers = new JSONObject()
    rails.getList.foreach(instance => {
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
    
    haproxy.getList.foreach(instance => {
      val server = new JSONObject()
      server.put("start", 4000)
      server.put("count", 1)
      nginxNginxServers.put(instance.privateDnsName, server)
    })
    nginxNginx.put("servers", nginxNginxServers)
    nginxConfig.put("nginx", nginxNginx)
    
    val nginxFaban = new JSONObject()
    nginxFaban.put("mysql", false)
    nginxConfig.put("faban", nginxFaban)
    
    /* faban configuration */
    val fabanConfig = new JSONObject()
    fabanConfig.put("recipes", new JSONArray().put("cloudstone::faban"))
    val fabanFaban = new JSONObject()
    val fabanFabanHosts = new JSONObject()
    fabanFabanHosts.put("driver", faban.getList.head.privateDnsName)
    fabanFabanHosts.put("webserver", nginx.getList.head.privateDnsName)
    fabanFabanHosts.put("database", mysql.getList.head.privateDnsName)
    fabanFabanHosts.put("storage", "")
    fabanFabanHosts.put("cache", "")
    
    fabanFaban.put("hosts", fabanFabanHosts)
    fabanConfig.put("faban", fabanFaban)
    
    /* faban-agent config */
    
    def deployMaker(jsonConfig: JSONObject): (Instance) => Unit = {
      (instance: Instance) => {
        instance.deploy(jsonConfig)
      }
    }
    
    mysql.parallelExecute(deployMaker(mysqlConfig))
    rails.parallelExecute(deployMaker(railsConfig))
    haproxy.parallelExecute(deployMaker(haproxyConfig))
    nginx.parallelExecute(deployMaker(nginxConfig))
    faban.parallelExecute(deployMaker(fabanConfig))
    
    def startAllServices(instance: Instance): Unit = {
      def startService(service: Service): Unit = {service.start}
      instance.getAllServices.foreach(startService)
    }
    
    //allInstances.parallelExecute(startAllServices)
    println("All done")
  }
}


