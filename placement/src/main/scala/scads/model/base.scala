package edu.berkeley.cs.scads.model

import scala.collection.mutable.ArrayBuffer
import java.text.ParsePosition
import org.apache.log4j.Logger

/**
 * The entity class provides helper methods for accessing/modifying the attributes of a single object stored in SCADS.
 * A developer needs to provide concrete implementations of the following instance variables:
 * <ul>
 * <li>namespace - the namespace that this entity will be stored in</li>
 * <li>primaryKey - a reference to the <code>Field</code> that will be used as a primary key.  The key can be composed of more than field by instantiating the <code>CompositeField</code> class.
 * <li>version - a <code>Version</li> class that specifies the type of versioning that will be used to track modifications to a specific instance of this entity, or <code>Unversioned</code>.
 * <li>attributes - A <code>String</code> to <code>Field</code> mapping that lists the names of actual fields that will be stored when serializing or deserializing this entity.</li>
 * <li>indexes - A list of <code>Index</code> objects that should be updated when this entity is created/modified.</li>
 * </ul>
 * @author Michael Armbrust
 */
abstract class Entity(implicit env: Environment) {
	val logger = Logger.getLogger("scads.entity")
	val namespace: String
	val primaryKey: Field
	val version: Version
	val attributes: scala.collection.Map[String, Field]
	val indexes: Seq[Index]

	/**
	 * Serialize all of the fields that are stored in the attributes map to a string.
	 */
	def serializeAttributes: String = {
		val builder = new StringBuilder()
		val attributeName = new StringField()

		attributes.foreach((f) => {
			attributeName(f._1)
			builder.append(attributeName.serialize)
			builder.append(f._2.serialize)
		})

		builder.toString()
	}

	/**
	 * Deserialize from the provided string into the actual fields in the attributes map of this instance.
	 */
	def deserializeAttributes(data: String) = {
		val pos = new ParsePosition(0)
		val attributeName = new StringField()

		while(pos.getIndex < data.length) {
			attributeName.deserialize(data, pos)
			attributes(attributeName.value).deserialize(data,pos)
		}

		indexes.foreach(_.initalize)
	}

	/**
	 * Create/update this instance of the entity type in the underlying store, and update all indexes.
	 */
	def save = {
		val otherActions = new ArrayBuffer[Action]
		val updateRecord = new VersionedWriteAll(namespace, primaryKey, version, serializeAttributes)

		indexes.foreach(otherActions ++ _.updateActions)
		env.executor.execute(updateRecord, otherActions)

		version.increment
		indexes.foreach(_.initalize)
	}

	/**
	 * Delete this instance of the entity type in the underlying store, and all indexes that point to it
	 * TODO
	 */
	def delete = null
}
