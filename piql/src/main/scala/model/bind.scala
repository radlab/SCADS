package edu.berkeley.cs.scads.piql.parser

import org.apache.log4j.Logger
import scala.collection.mutable.HashMap
import org.apache.avro.Schema
import scala.collection.jcl.Conversions._


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
case class InternalBindingError(desc: String) extends BindingException


class Binder(spec: Spec) {
	val logger = Logger.getLogger("scads.binding")

	/* Temporary object used while building the fetchTree */
	case class Fetch(entity: BoundEntity, child: Option[Fetch], relation: Option[BoundRelationship]) {
		val predicates = new scala.collection.mutable.ArrayBuffer[BoundPredicate]
		var orderField:Option[String] = None
		var orderDirection: Option[Direction] = None
	}

	def bind: BoundSpec = {
		/* Bind entities into a map and check for duplicate names */
		val boundEntities = bindEntities(spec.entities, Nil)

		logger.debug("===Entities===")
		boundEntities.foreach(logger.debug)

		/* Process all the queries and place them either in the orphan map, or the BoundEntity they belong to*/
		val orphanQueryMap = new HashMap[String, BoundQuery]()
		spec.queries.foreach((q) => {
			logger.debug("===Begining Bind of Query: " + q.name + "===")
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
			val parameters = Set(allParameters: _*).toList.sort(_.ordinal < _.ordinal)

			/* Ensure any duplicate parameter names are actually the same parameter */
			if(parameters.size != Set(allParameters.map(_.name): _*).size)
				throw new DuplicateParameterException(q.name)

			/* Ensure that parameter ordinals are contiguious starting at 1 */
			parameters.foldLeft(1)((o: Int, p: Parameter) => {
				logger.debug("Ordinal checking, found " + p + " expected " + o)
				if(p.ordinal != o)
					throw new BadParameterOrdinals(q.name)
				o + 1
			})
			logger.debug("Final parameter list: " + parameters)

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
				val entity = boundEntities.find(_.name equals j.entity) match {
					case Some(e) => e
					case None => throw new UnknownEntityException(j.entity)
				}

				/* Optionally resolve the relationship to the child */
				val relationship: Option[BoundRelationship] = child._2 match {
					case None => None
					case Some(relName) => entity.relationships.find(r => (r.target equals child._1.get.entity.name) && (r.name equals relName)) match {
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
			def getBoundParam(name: String, aType: Schema):BoundParameter = {
				boundParams.get(name) match {
					case None => {
						val bp = BoundParameter(name, aType)
						boundParams.put(name,bp)
						bp
					}
					case Some(bp) => {
						if(!(bp.schema equals aType)) throw InconsistentParameterTyping(q.name, name)
						bp
					}
				}
			}

			def addAttrEquality(f: AttributeValue, value: FixedValue) {
				val fetch = resolveField(f)
				fetch.predicates.append(
					AttributeEqualityPredicate(f.name, value match {
						case Parameter(name, _) =>  getBoundParam(name, fetch.entity.attributes(f.name))
						case StringValue(value) => BoundStringValue(value)
						case NumberValue(value) => BoundIntegerValue(value)
						case TrueValue => BoundTrueValue
						case FalseValue => BoundFalseValue
					})
				)
			}

			def addThisEquality(alias: String) {
				val fetch = resolveFetch(alias)
				fetch.entity.keySchema.getFields.map(_.name).foreach((k) => fetch.predicates.append(AttributeEqualityPredicate(k, BoundThisAttribute(k, fetch.entity.attributes(k)))))
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

			def toBoundFetch(f: Fetch): BoundFetch = {
				val order:BoundOrder = (f.orderField, f.orderDirection) match {
					case (Some(of), Some(Ascending)) => Sorted(of, true)
					case (Some(of), Some(Descending)) => Sorted(of, false)
					case (None, None) => Unsorted
					case _ => throw new InternalBindingError("Inconsistent sort parameters: " + f)
				}

				val join: BoundJoin = (f.child, f.relation) match {
					case (Some(ch), Some(BoundRelationship(name, target, _, ForeignKeyTarget))) => BoundPointerJoin(name, toBoundFetch(ch))
					case (Some(ch), Some(BoundRelationship(name, target, FixedCardinality(c), ForeignKeyHolder))) => BoundFixedTargetJoin(name, c, toBoundFetch(ch))
					case (Some(ch), Some(BoundRelationship(name, target, InfiniteCardinality, ForeignKeyHolder))) => BoundInfiniteTargetJoin(name, toBoundFetch(ch))
					case (None, None) => NoJoin
					case _ => {
						logger.fatal("Internal error: " + (f.child, f.relation))
						throw new InternalBindingError("Inconsistent child fetch: " + f)
					}
				}

				BoundFetch(f.entity, f.predicates.toList, order, join)
			}

			/* Bind the range expression */
			val boundRange = q.range match {
				case Unlimited => BoundUnlimited
				case Limit(Parameter(name,_), max) => BoundLimit(getBoundParam(name, Schema.create(Schema.Type.INT)), max)
				case Limit(NumberValue(v), max) => BoundLimit(BoundIntegerValue(v), max)
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

		return BoundSpec(Map(boundEntities.map(e => e.name -> e):_*), orphanQueryMap)
	}


	/* attempt to bind entities.  if we need to know a pkType for a fk relationship and thats
			not already done, calculate it then try again */
	def bindEntities(untouched: List[Entity], done: List[BoundEntity]): List[BoundEntity] = {
		if(untouched.size == 0)
			return done

		val currentEntity = untouched.first
		val fkDependencies = currentEntity.attributes.flatMap {
      case a: ForeignKey => List(a)
      case _ => Nil
    }
    val unmetFkDependencies = fkDependencies.filter(k => !done.map(_.name).contains(k.foreignType))

		val reqEntities =
			if(unmetFkDependencies.size == 0)
				Nil
			else
				bindEntities(
					untouched.drop(1),
					done)

		val donePrime = done ++ reqEntities
		val doneNames = donePrime.map(_.name)

		val attributes = currentEntity.attributes.map {
      case SimpleAttribute(attrName, attrType) =>
        new Schema.Field(attrName, attrType match {
				  case StringType => Schema.create(Schema.Type.STRING)
				  case IntegerType => Schema.create(Schema.Type.INT)
				  case BooleanType => Schema.create(Schema.Type.BOOLEAN)
			  }, "", null)
      case ForeignKey(attrName, foreignType, _) => {
	      new Schema.Field(attrName, donePrime.find(_.name equals foreignType).get.keySchema, "", null)
      }
		}

		val keyParts = currentEntity.keys.map(k => attributes.find(k equals _.name).get)
		val valueParts = attributes.filter(a => !(currentEntity.keys contains a.name))

    val relationships = untouched.flatMap(e => {
      e.attributes.flatMap {
        case ForeignKey(attrName, currentEntity.name, cardinality) =>
          List(new BoundRelationship(attrName, e.name, cardinality, ForeignKeyHolder))
        case _ => Nil
      }
    }) ++
    done.flatMap(e => {
      e.relationships.flatMap {
        case BoundRelationship(name, currentEntity.name, cardinality, ForeignKeyHolder) =>
          List(BoundRelationship(name, e.name, cardinality, ForeignKeyTarget))
        case _ => Nil
      }
    })

		val keySchema = Schema.createRecord(currentEntity.name + "Key", "", "", false)
		keySchema.setFields(java.util.Arrays.asList(keyParts.toArray:_*))
		val valueSchema = Schema.createRecord(currentEntity.name + "Value", "", "", false)
		valueSchema.setFields(java.util.Arrays.asList(valueParts.toArray:_*))

		val boundEntity = new BoundEntity(
			currentEntity.name,
			keySchema,
			valueSchema,
			relationships)

		bindEntities(
			untouched.drop(1).filter(e => !(doneNames.contains(e.name))),
			donePrime + boundEntity
		)
	}
}
