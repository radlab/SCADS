package edu.berkeley.cs.scads.model.parser

import org.apache.log4j.Logger
import scala.collection.mutable.HashMap

class UnimplementedException extends Exception

abstract class Index
case class AttributeIndex(name: String, attributes: List[String]) extends Index
case class PrimaryIndex(name: String, attributes: List[String]) extends Index

/**
 * The optimizer takes in a BoundQuery and figures out how to satisfy it.
 * It will created indexes as needed.
 */
object Optimizer {
	val logger = Logger.getLogger("scads.optimizer")

	def optimize(query: BoundQuery):Unit = {
		try {
			query.plan = optimize(query.fetchTree)
			logger.debug("plan: " + query.plan)
		}
		catch {
			case e: UnimplementedException => logger.fatal("Couldn't optimize query " + e)
		}
	}

	def optimize(fetch: BoundFetch):Plan = {
		fetch match {
			case BoundFetch(entity, None, None, predicates, None, None) => {
				val equalityPredicates = predicates.map(_ match {
						case BoundEqualityPredicate(attributeName, value) => (attributeName, value)
						case BoundThisEqualityPredicate => throw new UnimplementedException
				})
				val valueMap = Map[String, FixedValue](equalityPredicates: _*)
				val equalityFields = Set(equalityPredicates.map((p) => p._1): _*)

				logger.debug("Finding an index for the following fields: " + equalityFields)
				val candidateIndexes = entity.indexes.toList.filter(
					_ match {
						case PrimaryIndex(_, fields) =>  Set(fields: _*) == equalityFields
						case _ => false
					}
				)

				logger.debug("Possible indexes: " + candidateIndexes)

				candidateIndexes(0) match {
					case PrimaryIndex(name, attrs) => PrimaryKeyGet(entity.name, attrs.map(valueMap(_)))
				}
			}
			case _ => throw new UnimplementedException
		}
	}
}
