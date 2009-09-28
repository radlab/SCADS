package edu.berkeley.cs.scads.model.parser

import org.apache.log4j.Logger

class UnimplementedException extends Exception

abstract class Index
case class AttributeIndex(attributes: List[String]) extends Index

/**
 * The optimizer takes in a BoundQuery and figures out how to satisfy it.
 * It will created indexes as needed.
 */
object Optimizer {
	val logger = Logger.getLogger("scads.optimizer")

	def optimize(query: BoundQuery):Unit = {
		optimize(query.fetchTree)
	}

	def optimize(fetch: BoundFetch):Unit = {
		fetch match {
			case BoundFetch(entity, None, None, predicates, None, None) => {
				val equalityPredicates = predicates.map(_ match {
						case BoundEqualityPredicate(attributeName, value) => (attributeName, value)
						case BoundThisEqualityPredicate => throw new UnimplementedException
				})
				val equalityFields = Set(equalityPredicates.map((p) => p._1): _*)

				logger.debug("Finding an index for the following fields: " + equalityFields)
				val candidateIndexes = entity.indexes.toList.filter((i) => {
					i._2 match {
						case AttributeIndex(fields) => Set(fields: _*) == equalityFields
						case _ => false
					}
				})

				logger.debug("Possible indexes: " + candidateIndexes)
			}
			case _ => throw new UnimplementedException
		}
	}
}
