package deploylib.runit

import java.io.File

object EchoTest extends RunitTemplate {
	val name = "echotest"
	val command = "echo 'hello'"
}

object ScadsEngine extends JavaService(new File("/Users/marmbrus/Workspace/scads/scalaengine/target/scalaengine-1.0-SNAPSHOT-jar-with-dependencies.jar"), "edu.berkeley.cs.scads.storage.JavaEngine", "")
