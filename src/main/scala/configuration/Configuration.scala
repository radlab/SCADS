package deploylib.configuration

import scala.collection.mutable.ListBuffer
import deploylib.configuration.ValueConverstion._
import java.io.File


/* Structure of a configuration Node */
abstract class Configuration {
	def action(target: RemoteMachine)
	def description: String
}

abstract class CompositeConfiguration extends Configuration {
	val children = new ListBuffer[Configuration]

	def action(target: RemoteMachine) = {
		children.foreach({(c) =>
			//logger.debug("Executing: " + c)
			c.action(target)
		})
	}
}
