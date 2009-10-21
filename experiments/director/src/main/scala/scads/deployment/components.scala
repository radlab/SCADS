package scads.deployment

abstract class Component {
	import java.io._
	val dir = "/tmp/"

	protected def logBoots(lengths: List[Long], file_suffix:String) = writeLog(dir+"boot-"+file_suffix+".csv",lengths.mkString("\n")+"\n")
	protected def logDeploy(lengths: List[Long], file_suffix:String) = writeLog(dir+"deploy-"+file_suffix+".csv",lengths.mkString("\n")+"\n")
	protected def writeLog(filename:String, values:String) = {
		val logwriter = new FileWriter( new java.io.File(filename), true )
		logwriter.write(values)
		logwriter.flush()
		logwriter.close()
	}

	def boot
	def waitUntilBooted
	def deploy
	def waitUntilDeployed
}
