/**
 * Test classes that use reflection tricks to make it slightly easier to define new entities.
 * This will probably be discontinued as its slow, not super pretty, and the compiler will be generating this code anyway.
 */

package edu.berkeley.cs.scads.model.reflected

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import edu.berkeley.cs.scads.model.Field
import edu.berkeley.cs.scads.model.Index

import org.apache.log4j.Logger


abstract class Entity(implicit env: Environment) extends edu.berkeley.cs.scads.model.Entity {
	val namespace = this.getClass().getSimpleName()
	val attributes = new HashMap[String, edu.berkeley.cs.scads.model.Field]()
	val indexes = new ArrayBuffer[edu.berkeley.cs.scads.model.Index]()
	val version = new edu.berkeley.cs.scads.model.IntegerVersion()
}

abstract trait AttributeAdder extends Field {
	val logger: Logger
	val parent: Entity

	logger.debug("Registering field: " + this.getClass().getSimpleName())
	parent.attributes.put(this.getClass().getSimpleName(), this)
}

case class StringField(parent: Entity) extends edu.berkeley.cs.scads.model.StringField with AttributeAdder

abstract trait IndexAdder extends Index {
	val target: Entity
	val name = this.getClass().getSimpleName()
	val field: Field
	target.indexes.append(this)
}

case class FieldIndex(target: Entity, field: Field) extends edu.berkeley.cs.scads.model.FieldIndex with IndexAdder
