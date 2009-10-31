package edu.berkeley.cs.scads.model.parser


import org.apache.log4j.Logger
import scala.collection.mutable.HashMap

case class UnimplementedException(desc: String) extends Exception

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

	def getPlan(query: BoundQuery):ExecutionNode = {
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

	def optimize(fetch: BoundFetch):ExecutionNode = {
		fetch match {
			case BoundFetch(entity, None, None, predicates, None, None) => {

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

				/* Find candidate indexes by looking for prefix matches of attributes */
				val candidateIndexes = entity.indexes.filter((i) => {
					i.attributes.startsWith(equalityAttributes)
				})
				logger.debug("Identified candidate indexes: " + candidateIndexes)

        val selectedIndex =
				if(candidateIndexes.size == 0) {
					/* No index exists, so we must create one. */
					val idxName = "idx" + fetch.entity.name + equalityAttributes.mkString("", "_", "")
					val idxAttributes = equalityAttributes ++ (entity.keys -- equalityAttributes)
					val newIndex = new SecondaryIndex(idxName, idxAttributes, entity.namespace)
					logger.debug("Creating index on " + entity.name + " over attributes" + idxAttributes)
					entity.indexes.append(newIndex)
          newIndex
				}
        else {
          candidateIndexes(0)
        }

				def createLookupNode(ns: String, attrs: List[String], equalityFieldAttributeMap: HashMap[String, Field], versionType: Version): TupleProvider = {
					/* If the index is over more attributes than the equality we need to do a prefix match */
					if(attrs.size > equalityFieldAttributeMap.size) {
						val prefix = CompositeField(attrs.slice(0, equalityFieldAttributeMap.size).map(equalityFieldAttributeMap):_*)
						new PrefixGet(ns, prefix, 100, prefix, versionType) with ReadOneSetGetter
					}
					else {
						new SingleGet(ns, CompositeField(attrs.map(equalityFieldAttributeMap):_*), versionType) with ReadOneGetter
					}
				}

        val tupleStream = selectedIndex match {
          case PrimaryIndex(ns, attrs) => {
							createLookupNode(ns, attrs, equalityAttributeFieldMap, new IntegerVersion)
          }
          case SecondaryIndex(ns, attrs, tns) => {
						new SequentialDereferenceIndex(tns, entity.pkType.toField, new IntegerVersion,
							createLookupNode(ns, attrs, equalityAttributeFieldMap, Unversioned)
						) with ReadOneGetter
					}
        }
				new Materialize(tupleStream)(scala.reflect.Manifest.classType(getClass(entity.name)))
			}
			case _ => throw UnimplementedException("I don't know what to do w/ this fetch: " + fetch)
		}
	}

	//FIXME: Use the actual compiled entities instead of these placeholders
  def getClass(entityName:String) = {
		val compiler = new ScalaCompiler

    compiler.compile("class " + entityName + """
    extends edu.berkeley.cs.scads.model.Entity()(null) {
    val namespace = "Placeholder"
    val primaryKey: edu.berkeley.cs.scads.model.Field = null
    val attributes = Map[String, edu.berkeley.cs.scads.model.Field]()
    val indexes = Array[edu.berkeley.cs.scads.model.Index]()
    val version = edu.berkeley.cs.scads.model.Unversioned
    }""")

    compiler.classLoader.loadClass(entityName).asInstanceOf[Class[edu.berkeley.cs.scads.model.Entity]]
  }
}
