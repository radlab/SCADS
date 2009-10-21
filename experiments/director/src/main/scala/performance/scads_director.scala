/**
 * scripts for deploying the director
 */

package performance

import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import com.twitter.commons._
import scala.collection.jcl.Conversions._

object ScadsDirector {

  	var directorVM: Instance = null
	var config: JsonQuoted = null

  	def startDirectorVM {
    	directorVM = DataCenter.runInstances(1, "m1.small").getFirst()
	    directorVM.waitUntilReady

		config = Json.build( Map("recipes"->Array("director::scads_director"),
									 "director"->Map("basedir"->"/mnt/director",
													 "metricService"->Map("port"->6001,"dbhost"->"localhost","dbuser"->"root","dbpassword"->"","dbname"->"metrics"))))
	    println( config.toString() )
	    directorVM.deploy(config)

		// add aws environment vars to bash_profile
		val local = Array[String]("aws.cfg")
		directorVM.upload(local,"/opt/scads/experiments")
		directorVM.exec("cat /opt/scads/experiments/aws.cfg >> /root/.bash_profile")
  	}

}
