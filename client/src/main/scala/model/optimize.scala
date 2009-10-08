package edu.berkeley.cs.scads.model.parser

import org.apache.log4j.Logger
import scala.collection.mutable.HashMap

class UnimplementedException(desc: String) extends Exception

abstract class IndexValueType
object Primary extends IndexValueType
object Pointer extends IndexValueType

abstract class Index
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
			case BoundFetch(entity, None, None, predicates, None, None) => null 
			case _ => null
		}
	}
}
