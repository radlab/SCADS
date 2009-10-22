package edu.berkeley.cs.scads.model

import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger

/**
 * Abstract definition of a secondary index.
 */
abstract class Index {
	val logger = Logger.getLogger("scads.index")

	/**
	 * Perform any book keeping necessary to initialize this instance. Ex: remember the original value of a field being indexed
	 * This method will be called anytime a previously stored set of fields is loaded
	 */
	def initalize: Unit

	/**
	 * Return a list of actions that would need to be executed in order to keep this index updated based on changes to the data it is indexing.
	 */
	def updateActions: Seq[Action]

	/**
	 * Return a list of actions that would need to be executed if the data indexed is deleted.
	 */
	def deleteActions: Seq[Action]
}

/**
 * Creates a simple secondary index over a single field.
 * The user needs to specify:
 * <ul>
 * <li>name - the namespace that the index will be stored in</li>
 * <li>target - the entity that is the target of the secondary index</li>
 * <li>field - the field that the index is being created over</li>
 * </ul>
 */
class AttributeIndex(name: String, target: Entity, attr: Field) extends Index {
	var oldValue: Field = null

	def initalize: Unit = {
		oldValue = attr.duplicate()
	}

	def updateActions: Seq[Action] = {
		val actions = new ArrayBuffer[Action]()

		if(oldValue != attr)
			actions += (new WriteAll(name, attr, target.primaryKey.serialize))
		if(oldValue != null && oldValue != attr)
			actions += (new WriteAll(name, oldValue, null))

		logger.debug("Index actions for " + oldValue + " to " + attr + ":" + actions)

		return actions
	}

	def deleteActions: Seq[Action] = null
}
