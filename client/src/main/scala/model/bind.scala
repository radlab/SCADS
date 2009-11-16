package edu.berkeley.cs.scads.model.parser

import org.apache.log4j.Logger
import scala.collection.mutable.HashMap
import edu.berkeley.cs.scads.model.{StringField, IntegerField, BooleanField}

/* Exceptions that can occur during binding */
sealed class BindingException extends Exception
case class DuplicateEntityException(entityName: String) extends BindingException
case class DuplicateAttributeException(entityName: String, attributeName: String) extends BindingException
case class DuplicateRelationException(relationName: String) extends BindingException
case class UnknownEntityException(entityName: String) extends BindingException
case class DuplicateQueryException(queryName: String) extends BindingException
case class DuplicateParameterException(queryName: String) extends BindingException
case class BadParameterOrdinals(queryName:String) extends BindingException
case class AmbigiousThisParameter(queryName: String) extends BindingException
case class UnknownRelationshipException(queryName :String) extends BindingException
case class AmbiguiousJoinAlias(queryName: String, alias: String) extends BindingException
case class UnsupportedPredicateException(queryName: String, predicate: Predicate) extends BindingException
case class AmbiguiousAttribute(queryName: String, attribute: String) extends BindingException
case class UnknownAttributeException(queryName: String, attribute: String) extends BindingException
case class UnknownFetchAlias(queryName: String, alias: String) extends BindingException
case class InconsistentParameterTyping(queryName: String, paramName: String) extends BindingException
case class InvalidPrimaryKeyException(entityName: String, keyPart:String) extends BindingException

/* Temporary object used while building the fetchTree */
case class Fetch(entity: BoundEntity, child: Option[Fetch], relation: Option[BoundRelationship]) {
	val predicates = new scala.collection.mutable.ArrayBuffer[BoundPredicate]
	var orderField:Option[String] = None
	var orderDirection: Option[Direction] = None
}

object Binder {
	val logger = Logger.getLogger("scads.binding")

	def bind(spec: Spec):BoundSpec = {
		/* Bind entities into a map and check for duplicate names */
		val entityMap = new HashMap[String, BoundEntity]()
		spec.entities.foreach((e) => {
			entityMap.get(e.name) match
			{
				case Some(_) => throw new DuplicateEntityException(e.name)
				case None => entityMap.put(e.name, new BoundEntity(e))
			}
		})

		/* Add primary key as an index */
		entityMap.values.foreach(e => {
			e.indexes += new PrimaryIndex(e.namespace, e.keys)
		})

		/* Helper function for getting Entities */
		def getEntity(name: String): BoundEntity = {
			entityMap.get(name) match {
				case None => throw new UnknownEntityException(name)
				case Some(entity) => entity
			}
		}

		/* Bind relationships to the entities they link, check for bad entity names and duplicate relationship names */
		spec.relationships.foreach((r) => {
			val from = getEntity(r.from)
			val to = getEntity(r.to)

			if(from.relationships.put((to.name, r.name), new BoundRelationship(r.name, to, r.cardinality, ForeignKeyTarget)).isDefined)
		 		throw DuplicateRelationException(r.name)

			if(to.relationships.put((from.name, r.name), new BoundRelationship(r.name, from, r.cardinality, ForeignKeyHolder)).isDefined)
				throw DuplicateRelationException(r.name)

			if(to.attributes.put(r.name, from.pkType).isDefined)
				throw DuplicateAttributeException(to.name, r.name)
		})

		/* Check the primary keys of all entities */
		entityMap.foreach(_._2.pkType)

		/* Process all the queries and place them either in the orphan map, or the BoundEntity they belong to*/
		val orphanQueryMap = new HashMap[String, BoundQuery]()
		spec.queries.foreach((q) => {
			/* Extract all Parameters from Predicates */
			val predParameters: List[Parameter] =
				q.predicates.map(
					_ match {
						case EqualityPredicate(op1, op2) => {
							val p1 = op1 match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
							val p2 = op2 match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
							p1 ++ p2
						}
					}).flatten
			/* Extract possible parameter from the limit clause */
			val limitParameters: Array[Parameter] =
				q.range match {
					case Limit(p, _) => p match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
					case Paginate(p, _) => p match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
					case Unlimited => Array[Parameter]()
				}
			val allParameters = predParameters ++ limitParameters
			val parameters = Set(allParameters: _*).toList.sort(_.ordinal > _.ordinal)

			/* Ensure any duplicate parameter names are actually the same parameter */
			if(parameters.size != Set(allParameters.map(_.name): _*).size)
				throw new DuplicateParameterException(q.name)

			/* Ensure that parameter ordinals are contiguious starting at 1 */
			parameters.foldRight(1)((p: Parameter, o: Int) => {
				logger.debug("Ordinal checking, found " + p + " expected " + o)
				if(p.ordinal != o)
					throw new BadParameterOrdinals(q.name)
				o + 1
			})


			/* Build the fetch tree and alias map */
			val fetchAliases = new HashMap[String, Fetch]()
			val duplicateAliases = new scala.collection.mutable.HashSet[String]()

			val attributeMap = new HashMap[String, Fetch]()
			val duplicateAttributes = new scala.collection.mutable.HashSet[String]()

			val fetchTree: Fetch = q.joins.foldRight[(Option[Fetch], Option[String])]((None,None))((j: Join, child: (Option[Fetch], Option[String])) => {
				logger.debug("Looking for relationship " + child._2 + " in " + j + " with child " + child._1)
				if(child._2.isDefined)
					logger.debug("Candidates: " + child._1.get.entity.relationships)

				/* Resolve the entity for this fetch */
				val entity = entityMap.get(j.entity) match {
					case Some(e) => e
					case None => throw new UnknownEntityException(j.entity)
				}

				/* Optionally resolve the relationship to the child */
				val relationship: Option[BoundRelationship] = child._2 match {
					case None => None
					case Some(relName) => entity.relationships.get((child._1.get.entity.name, relName)) match {
						case Some(rel) => Some(rel)
						case None => throw new UnknownRelationshipException(relName)
					}
				}

				/* Create the Fetch */
				val fetch = new Fetch(entity, child._1, relationship)

				/* Convert the relationship to an option for passing the parent */
				val relToParent = if(j.relationship == null)
					None
				else
					Some(j.relationship)

				/* Build lookup tables for fetch aliases and unambiguious attributes */
				if(!duplicateAliases.contains(j.entity)) {
					fetchAliases.get(j.entity) match {
						case None => fetchAliases.put(j.entity, fetch)
						case Some(_) => {
							logger.debug("Fetch alias " + j.entity + " is ambiguious in query " + q.name + " and therefore can't be used in predicates")
							fetchAliases -= j.entity
							duplicateAliases += j.entity
						}
					}
				}
				if(j.alias != null)
					fetchAliases.get(j.alias) match {
						case None => fetchAliases.put(j.alias, fetch)
						case Some(_) => throw new AmbiguiousJoinAlias(q.name, j.alias)
					}

				entity.attributes.keys.foreach((a) => {
					if(!duplicateAttributes.contains(a))
						attributeMap.get(a) match {
							case None => attributeMap.put(a, fetch)
							case Some(_) => {
								logger.debug("Attribute " + a + " is ambiguious in query " + q.name + " and therefore can't be used in predicates without a fetch specifier")
								attributeMap -= a
								duplicateAttributes += a
							}
						}
				})

				/* Pass result to our parent */
				(Some(fetch), relToParent)
			})._1.get
			logger.debug("Generated fetch tree for " + q.name + ": " + fetchTree)

			/* Check this parameter typing */
			val thisTypes: List[String] = q.predicates.map(
				_ match {
					case EqualityPredicate(AttributeValue(null, thisType), ThisParameter) => Array(thisType)
					case EqualityPredicate(ThisParameter, AttributeValue(null, thisType)) => Array(thisType)
					case _ => Array[String]()
				}).flatten
			logger.debug("this types detected for " + q.name + ": " + Set(thisTypes))

			val thisType: Option[String] = Set(thisTypes: _*).size match {
					case 0 => None
					case 1 => Some(thisTypes.head)
					case _ => throw new AmbigiousThisParameter(q.name)
				}
			logger.debug("Detected thisType of " + thisType + " for query " + q.name)

			/* Helper functions for identifying the Fetch that is being referenced in a given predicate */
			def resolveFetch(alias: String):Fetch = {
				if(duplicateAliases.contains(alias))
					throw new AmbiguiousJoinAlias(q.name, alias)
				fetchAliases.get(alias) match {
					case None => throw UnknownFetchAlias(q.name, alias)
					case Some(bf) => bf
				}
			}

			def resolveField(f:AttributeValue):Fetch = {
				if(f.entity == null) {
					if(duplicateAttributes.contains(f.name))
						throw new AmbiguiousAttribute(q.name, f.name)
					return attributeMap.get(f.name) match {
						case None => throw new UnknownAttributeException(q.name, f.name)
						case Some(bf) => bf
					}
				}
				else {
					val bf = resolveFetch(f.entity)
					if(!bf.entity.attributes.contains(f.name))
						throw UnknownAttributeException(q.name, f.name)
					return bf
				}
			}

			/* Helper function for assigning and validating parameter types */
			val boundParams = new HashMap[String, BoundParameter]
			def getBoundParam(name: String, aType: AttributeType):BoundParameter = {
				boundParams.get(name) match {
					case None => {
						val bp = BoundParameter(name, aType)
						boundParams.put(name,bp)
						bp
					}
					case Some(bp) => {
						if(bp.aType != aType) throw InconsistentParameterTyping(q.name, name)
						bp
					}
				}
			}

			def addAttrEquality(f: AttributeValue, value: FixedValue) {
				val fetch = resolveField(f)
				fetch.predicates.append(
					AttributeEqualityPredicate(f.name, value match {
						case Parameter(name, _) =>  getBoundParam(name, fetch.entity.attributes(f.name))
						case StringValue(value) => StringField(value)
						case NumberValue(value) => IntegerField(value)
						case TrueValue => TrueField
						case FalseValue => FalseField
					})
				)
			}

			def addThisEquality(alias: String) {
				val fetch = resolveFetch(alias)
				fetch.entity.keys.foreach((k) => fetch.predicates.append(AttributeEqualityPredicate(k, BoundThisAttribute(k, fetch.entity.attributes(k)))))
			}

			/* Bind predicates to the proper node of the Fetch Tree */
			q.predicates.foreach( _ match {
				case EqualityPredicate(AttributeValue(null, alias), ThisParameter) => addThisEquality(alias)
			  case EqualityPredicate(ThisParameter, AttributeValue(null, alias)) => addThisEquality(alias)
				case EqualityPredicate(f: AttributeValue, v: FixedValue) => addAttrEquality(f,v)
				case EqualityPredicate(v :FixedValue, f: AttributeValue
        ) => addAttrEquality(f,v)
				case usp: Predicate => throw UnsupportedPredicateException(q.name, usp)
			})

			/* Bind the ORDER BY clause */
			q.order match {
				case Unordered => null
				case OrderedByField(field, direction) => {
					val fetch = resolveField(field)
					fetch.orderField = Some(field.name)
					fetch.orderDirection = Some(direction)
				}
			}

			def toBoundFetch(f: Fetch): BoundFetch =
				f.child match {
					case Some(child) => BoundFetch(f.entity, Some(toBoundFetch(child)), f.relation, f.predicates.toList, f.orderField, f.orderDirection)
					case None => BoundFetch(f.entity, None, None, f.predicates.toList, f.orderField, f.orderDirection)
			}

			/* Bind the range expression */
			val boundRange = q.range match {
				case Unlimited => BoundUnlimited
				case Limit(Parameter(name,_), max) => BoundLimit(getBoundParam(name, IntegerType), max)
				case Limit(NumberValue(v), max) => BoundLimit(IntegerField(v), max)
			}

			/* Build the final bound query */
			val boundQuery = BoundQuery(toBoundFetch(fetchTree), parameters.map((p) => boundParams(p.name)), boundRange)

			/* Place bound query either in its entity or as an orphan */
			val placement: HashMap[String, BoundQuery] = thisType match {
				case None => orphanQueryMap
				case Some(parentFetchName) => resolveFetch(parentFetchName).entity.queries
			}
			placement.get(q.name) match {
				case None => placement.put(q.name, boundQuery)
				case Some(_) => throw DuplicateQueryException(q.name)
			}
		})

		return BoundSpec(entityMap, orphanQueryMap)
	}

}
