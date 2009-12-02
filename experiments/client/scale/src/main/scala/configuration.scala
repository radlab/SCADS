package scaletest

import deploylib.configuration.JavaService
import deploylib.configuration.ValueConverstion._

class ScadsEngine(port: Int, zooServer: String) extends JavaService(
    "../../../scalaengine/target/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar",
    "edu.berkeley.cs.scads.storage.JavaEngine",
    "-p " + port + " -z " + zooServer)


