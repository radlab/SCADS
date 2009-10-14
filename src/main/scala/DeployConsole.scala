package deploylib

import scala.tools.nsc.MainGenericRunner
import deploylib._

object DeployConsole {
   def main(args : Array[String]) {
     MainGenericRunner.main(Array("-i", "setup.scala") ++ args)
     // After the repl exits, then exit the scala script
     exit(0)
   }
}
