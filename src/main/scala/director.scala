/**
 * scripts for deploying the director
 */
 
package DirectorDeployment
 
import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

object Director {
  
  def runInstances(count: Int, typeString: String): InstanceGroup = {
     val imageId = InstanceType.bits(typeString) match {
       case "32-bit" => "ami-e7a2448e"
       case "64-bit" => "ami-e4a2448d"
     }
     val keyName = "bodikp-keypair"
     val keyPath = "/Users/bodikp/.ec2/bodikp-keypair"
     val location = "us-east-1a"
     
     DataCenter.runInstances(imageId, count, keyName, keyPath, typeString, location)
  }
  
  def startDirectorVM: Instance = {
    val directorVM = runInstances(1, "m1.small").getFirst()
    
    println("Waiting until all instances are ready.")
    directorVM.waitUntilReady
    directorVM
  }
  
  def configureDirectorVM(directorVM: Instance) = {
    val config = """  
    {
        "mysql": {
            "optimized": false,
            "username": "director",
            "password": "director"
        },
        "director": {
            "basedir": "/mnt/director/",
            "url_to_repo": "http://s3.amazonaws.com/radlab_director/radlab-repos-6faba3efb146172c3da676f7dc899acf072b6a69.tgz",
            "metricService": {
                "port": 6001,
                "dbuser": "director",
                "dbpassword": "director",
                "dbname": "metrics"
            }
        },
        "recipes": ["cloudstone::mysql", "director::director"]
    }
    """
    
    val jsonCfg = new JSONObject(config)
    jsonCfg.toString(4)
    directorVM.deploy(jsonCfg)
  }
  
  def startHAProxyVM: Instance = {
    val vm = runInstances(1, "m1.small").getFirst()
    println("Waiting until HAProxy VM is ready.") 
    vm.waitUntilReady
    vm
  }
  
  def testHAProxyLogging(directorIP: String, vm: Instance) = {
    val config = """  
    {
        "haproxy": {
            "basedir": "/mnt/director/",
            "metricService": {
                "host": "FILLIN",
                "port": 6001
            },
            "port": 4000,
            "servers": {
                "www.cnn.com_rails_cnn.com_80": {
                    "start": 80,
                    "count": 1
                }
            },
        },
        "recipes": ["cloudstone::haproxy"]
    }
    """
    val jsonCfg = new JSONObject(config)
    jsonCfg.getJSONObject("haproxy").getJSONObject("metricService").put("host",directorIP)
    println( jsonCfg.toString(4) )
    vm.deploy(jsonCfg)
  }
  
}