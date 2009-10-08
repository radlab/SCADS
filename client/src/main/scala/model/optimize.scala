package edu.berkeley.cs.scads.model.parser

import org.apache.log4j.Logger
import scala.collection.mutable.HashMap

case class UnimplementedException(desc: String) extends Exception

sealed abstract class OptimizerException extends Exception
object Unsatisfiable extends OptimizerException

abstract sealed class IndexValueType
object PrimaryIndex extends IndexValueType
case class PointerIndex(dest: String) extends IndexValueType

sealed abstract class Index
case class AttributeKeyedIndex(namespace: String, attributes: List[String], idxType: IndexValueType) extends Index

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

				/* Map attributes to the values they should equal. Error contradicting predicates are found */
				val attrValueEqualityMap = new HashMap[String, BoundValue]
				predicates.map(_.asInstanceOf[AttributeEqualityPredicate]).foreach((p) => { //Note: We only handle equality
					attrValueEqualityMap.get(p.attributeName) match {
						case Some(value) => {
							if(value == p.value)
								logger.warn("Redundant equality found")
							else
								throw Unsatisfiable
						}
						case None => attrValueEqualityMap.put(p.attributeName, p.value)
					}
				})

				val equalityAttributes = attrValueEqualityMap.keys.toList

				/* Find candidate indexes by looking for prefix matches of attributes */
				val candidateIndexes = entity.indexes.map(_.asInstanceOf[AttributeKeyedIndex]).filter((i) => {
					i.attributes.startsWith(equalityAttributes)
				})
				logger.debug("Identified candidate indexes: " + candidateIndexes)

				if(candidateIndexes.size > 0)
					Materialize(entity.name, AttributeKeyedIndexGet(candidateIndexes(0), candidateIndexes(0).attributes.map(attrValueEqualityMap(_))))
				else
					null
			}
			case _ => throw UnimplementedException("I don't know what to do w/ this fetch: " + fetch)
		}
	}
}
