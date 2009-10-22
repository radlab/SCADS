package edu.berkeley.cs.scads.model.parser


import org.apache.log4j.Logger
import scala.collection.mutable.HashMap

case class UnimplementedException(desc: String) extends Exception

sealed abstract class OptimizerException extends Exception
object Unsatisfiable extends OptimizerException

sealed abstract class Index { val attributes: List[String] }
case class PrimaryIndex(attributes: List[String]) extends Index
case class SecondaryIndex(namespace: String, attributes: List[String]) extends Index

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

	def optimize(fetch: BoundFetch):ExecutionNode = {
		fetch match {
			case BoundFetch(entity, None, None, predicates, None, None) => {

				/* Map attributes to the values they should equal. Error contradicting predicates are found */
				val attrValueEqualityMap = new HashMap[String, Field]
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
				val candidateIndexes = entity.indexes.filter((i) => {
					i.attributes.startsWith(equalityAttributes)
				})
				logger.debug("Identified candidate indexes: " + candidateIndexes)

        val selectedIndex =
				if(candidateIndexes.size == 0) {
					/* No index exists, so we must create one. */
					val idxName = "idx" + fetch.entity.name + equalityAttributes.mkString("", "_", "")
					val newIndex = new SecondaryIndex(idxName, equalityAttributes)
					entity.indexes.append(newIndex)
          newIndex
				}
        else {
          candidateIndexes(0)
        }

        selectedIndex match {
          case PrimaryIndex(attrs) => {
            new Materialize(
              new SingleGet(entity.namespace, equalityAttributes.map(attrValueEqualityMap)(0), new IntegerVersion) with ReadOneGetter
            )(scala.reflect.Manifest.classType(getClass(entity.name)))
          }
          case SecondaryIndex(_,_) => null
        }
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
