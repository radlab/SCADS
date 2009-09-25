package edu.berkeley.cs.scads.model.parser

/**
 * Contains utility functions to figure out table to scads namespace mappings
 */
object Namespaces {
	def entity(entityName: String): String = "ent_" + entityName
}
