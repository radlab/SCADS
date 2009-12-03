package deploylib.configuration

import deploylib.configuration.ValueConverstion._

@deprecated
class JavaService(localJar: String, className: String, args: String, jvmArgs:String) extends CompositeConfiguration {

	def this(localJar: String, className: String, args: String) = 
		this(localJar,className,args,null)

	val remoteJar = new FileUpload(localJar, MachineRoot)
	val runCmd = jvmArgs match {
        case null => new LateStringBuilder(RunitService.header, "/usr/lib/jvm/java-6-sun/bin/java -cp .:", new LateBoundValue("JarLocation" ,remoteJar.getValue(_)), " ", className, " ", args)
        case args => new LateStringBuilder(RunitService.header, "/usr/lib/jvm/java-6-sun/bin/java ", args, " -cp .:", new LateBoundValue("JarLocation" ,remoteJar.getValue(_)), " ", className, " ", args) 
    }

	val service = new DefaultLoggedRunitService(className, runCmd)
	val log4jProperties = new RemoteFile(service.baseDirectory,
																			 "log4j.properties",
																			 Array("log4j.rootLogger=INFO, stdout",
																						 "log4j.appender.stdout=org.apache.log4j.ConsoleAppender",
																						 "log4j.appender.stdout.layout=org.apache.log4j.SimpleLayout").mkString("", "\n", "\n"), "644")


	children ++= List(remoteJar, service, log4jProperties)

	def description: String = "Create java service running " + className + " from " + localJar
}
