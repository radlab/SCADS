import scala.tools.nsc.MainGenericRunner
import org.apache.log4j.PropertyConfigurator

object ScadrConsole {
   def main(args : Array[String]) {
     PropertyConfigurator.configure("src/main/resources/log4j.console.properties")
     MainGenericRunner.main(Array("-i", "setup.scala") ++ args)
     // After the repl exits, then exit the scala script
     exit(0)
   }
}
