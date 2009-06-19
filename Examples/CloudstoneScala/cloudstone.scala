/**
 * An example use of the deployment library to deploy Cloudstone
 */
 
import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject


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
 
 
    val railsSettings   = (args(2).toInt, args(4))

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
      val location = "us-east-1a"
      
      DataCenter.runInstances(imageId, count, keyName, instance_type, location)
    }


    val rails   = runInstances(railsSettings._1, railsSettings._2)
    val mysql   = runInstances(mysqlSettings._1, mysqlSettings._2)
    val haproxy = runInstances(haproxySettings._1, haproxySettings._2)
    val nginx   = runInstances(nginxSettings._1, nginxSettings._2)
    val faban   = runInstances(fabanSettings._1, fabanSettings._2)
    
    val allInstances = rails ++ mysql ++ haproxy ++ nginx ++ faban
    
    /* A method to be passed into parallel execute. */
    def waitForAll(instance: Instance): Unit = { instance.waitUntilReady }
    
    allInstances.parallelExecute(waitForAll)
    
    val railsConfig: JSONObject = null
    val mysqlConfig: JSONObject = null
    val haproxyConfig: JSONObject = null
    val nginxConfig: JSONObject = null
    val fabanConfig: JSONObject = null
    
    /* Fill in values in configs */

    def deployMaker(jsonConfig: JSONObject): (Instance) => Unit = {
      def deployAll(instance: Instance): Unit = {
        instance.deploy(jsonConfig)
      }
      return deployAll
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
    
    allInstances.parallelExecute(startAllServices)
    
  }
}


