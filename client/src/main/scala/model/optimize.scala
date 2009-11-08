package edu.berkeley.cs.scads.model.parser


import org.apache.log4j.Logger
import scala.collection.mutable.HashMap

case class UnimplementedException(desc: String) extends Exception

abstract class UnboundedQuery extends Exception
class UnboundedFinalResult extends UnboundedQuery
class UnboundedIntermediateResult extends UnboundedQuery

sealed abstract class OptimizerException extends Exception
object Unsatisfiable extends OptimizerException

sealed abstract class Index {
	val namespace: String
	val attributes: List[String]
}

case class PrimaryIndex(namespace: String, attributes: List[String]) extends Index
case class SecondaryIndex(namespace: String, attributes: List[String], targetNamespace: String) extends Index

/**
 * The optimizer takes in a BoundQuery and figures out how to satisfy it.
 * It will created indexes as needed.
 */
class Optimizer(spec: BoundSpec) {
	val logger = Logger.getLogger("scads.optimizer")
	val compiler = new ScalaCompiler
	buildClasses()

	def optimizedSpec: BoundSpec = {
		spec.orphanQueries.values.foreach(query => {
			query.plan = getPlan(query)
		})

		spec.entities.values.foreach((entity) => {
			entity.queries.values.foreach((query) => {
				query.plan = getPlan(query)
			})
		})

		spec
	}

	def getPlan(query: BoundQuery):QueryPlan = {
		try {
			val plan = optimize(query.fetchTree)
			logger.debug("plan: " + query.plan)
			plan
		}
		catch {
			case e: UnimplementedException => {
				logger.fatal("Couldn't optimize query " + e)
				null
			}
		}
	}

	def optimize(fetch: BoundFetch):EntityProvider = {
		fetch match {
			case BoundFetch(entity, Some(child), Some(BoundRelationship(rname, rtarget, cardinality, ForeignKeyTarget)), Nil, _, _) => {
				Materialize(getClass(entity.name),
					PointerJoin(entity.namespace, List(rname), ReadRandomPolicy, optimize(child))
				)
			}
			case BoundFetch(entity, Some(child), Some(BoundRelationship(rname, rtarget, cardinality, ForeignKeyHolder)), Nil, _, _) => {
				val childPlan = optimize(child)
				logger.debug("Child Plan: " + childPlan)
				logger.debug("Relationship: " + rname)

				val selectedIndex = selectOrCreateIndex(entity, List(rname))
				logger.debug("Selected join index: " + selectedIndex)

				val tupleStream = selectedIndex match {
					case SecondaryIndex(ns, attrs, tns) => {
						SequentialDereferenceIndex(tns, ReadRandomPolicy,
							PrefixJoin(ns, rtarget.keys(0), 100, ReadRandomPolicy, childPlan)
						)
					}
					case _ => throw UnimplementedException("I don't know what to do w/ this fetch: " + fetch)
				}
				Materialize(getClass(entity.name), tupleStream)
			}
			case BoundFetch(entity, None, None, predicates, _, _) => {

				/* Map attributes to the values they should equal. Error contradicting predicates are found */
				val equalityAttributeFieldMap = new HashMap[String, Field]
				predicates.map(_.asInstanceOf[AttributeEqualityPredicate]).foreach((p) => { //Note: We only handle equality
					equalityAttributeFieldMap.get(p.attributeName) match {
						case Some(value) => {
							if(value == p.value)
								logger.warn("Redundant equality found")
							else
								throw Unsatisfiable
						}
						case None => equalityAttributeFieldMap.put(p.attributeName, p.value)
					}
				})

				val equalityAttributes = equalityAttributeFieldMap.keys.toList
				val selectedIndex = selectOrCreateIndex(entity, equalityAttributes)

				def createLookupNode(ns: String, attrs: List[String], equalityFieldAttributeMap: HashMap[String, Field], versionType: Version): TupleProvider = {
					/* If the index is over more attributes than the equality we need to do a prefix match */
					if(attrs.size > equalityFieldAttributeMap.size) {
						val prefix = CompositeField(attrs.slice(0, equalityFieldAttributeMap.size).map(equalityFieldAttributeMap):_*)
						PrefixGet(ns, prefix, 100, ReadRandomPolicy)
					}
					else {
						new SingleGet(ns, CompositeField(attrs.map(equalityFieldAttributeMap):_*), ReadRandomPolicy)
					}
				}

        val tupleStream = selectedIndex match {
          case PrimaryIndex(ns, attrs) => {
							createLookupNode(ns, attrs, equalityAttributeFieldMap, new IntegerVersion)
          }
          case SecondaryIndex(ns, attrs, tns) => {
						new SequentialDereferenceIndex(tns, ReadRandomPolicy,
							createLookupNode(ns, attrs, equalityAttributeFieldMap, Unversioned)
						)
					}
        }
				new Materialize(getClass(entity.name), tupleStream)
			}
			case _ => throw UnimplementedException("I don't know what to do w/ this fetch: " + fetch)
		}
	}

	protected def selectOrCreateIndex(entity: BoundEntity, attributes: List[String]): Index = {
		/* Find candidate indexes by looking for prefix matches of attributes */
		val candidateIndexes = entity.indexes.filter((i) => {
			i.attributes.startsWith(attributes)
		})
		logger.debug("Identified candidate indexes: " + candidateIndexes)

		if(candidateIndexes.size == 0) {
			/* No index exists, so we must create one. */
			val idxName = "idx" + entity.name + attributes.mkString("", "_", "")
			val idxAttributes = attributes ++ (entity.keys -- attributes)
			val newIndex = new SecondaryIndex(idxName, idxAttributes, entity.namespace)
			logger.debug("Creating index on " + entity.name + " over attributes" + idxAttributes)
			entity.indexes.append(newIndex)
			newIndex
			}
		else {
			candidateIndexes(0)
		}
	}

	private def buildClasses(): Unit = {
		val source = ScalaGen(spec)
		logger.debug("Creating Entity Placeholders")
		logger.debug(source)
		compiler.compile(source)
	}

	def getClass(entityName:String) = {
    compiler.classLoader.loadClass(entityName).asInstanceOf[Class[edu.berkeley.cs.scads.model.Entity]]
  }
}
