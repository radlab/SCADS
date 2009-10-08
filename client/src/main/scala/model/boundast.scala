package edu.berkeley.cs.scads.model.parser

import scala.collection.mutable.HashMap

/* Bound counterparts for some of the AST */
abstract sealed class BoundTree

/* SCADS Spec that has been bound and is ready to be optimized */
case class BoundSpec(entities: HashMap[String, BoundEntity], orphanQueries: HashMap[String, BoundQuery])

/* BoundEntity and any queries that depend on its ThisParameter */
case class BoundEntity(name: String, attributes: HashMap[String, AttributeType], keys: List[String]) {
	val relationships = new HashMap[String, BoundRelationship]()
	val queries = new HashMap[String, BoundQuery]()
	val indexes = new scala.collection.mutable.ArrayBuffer[Index]()

	def pkType:AttributeType ={
		val parts = keys.map((k) => attributes.get(k) match {
				case Some(at) => at
				case None => throw InvalidPrimaryKeyException(name, k)
		})

		if(parts.size == 1)
			parts(0)
		else
			new CompositeType(parts)
	}

	def this(e: Entity) {
		this(e.name, new HashMap[String, AttributeType](), e.keys)

		e.attributes.foreach((a) => {
			attributes.get(a.name) match {
				case Some(_) => throw new DuplicateAttributeException(e.name, a.name)
				case None => this.attributes.put(a.name, a.attrType)
			}
		})
	}
}

/* Bound Relationship */
case class BoundRelationship(target: String, cardinality: Cardinality)

/* BoundQuery and FetchTree */
case class BoundQuery(fetchTree: BoundFetch, parameters: List[BoundParameter], range:BoundRange) {var plan: Plan = null}
case class BoundFetch(entity: BoundEntity, child: Option[BoundFetch], relation: Option[BoundRelationship], predicates: List[BoundPredicate], orderField: Option[String], orderDirection: Option[Direction])

abstract class BoundRange
case class BoundLimit(lim: BoundValue, max: Int) extends BoundRange
object BoundUnlimited extends BoundRange

/* Bound Values */
abstract class BoundValue {val aType:AttributeType}
case class BoundParameter(name: String, aType: AttributeType) extends BoundValue
case class BoundThisAttribute(name: String, aType: AttributeType) extends BoundValue
abstract class BoundLiteral extends BoundValue
case class BoundStringLiteral(value: String) extends BoundLiteral {val aType = StringType}
case class BoundIntegerLiteral(value: Int) extends BoundLiteral {val aType = IntegerType}
object BoundTrue extends BoundLiteral {val aType = BooleanType}
object BoundFalse extends BoundLiteral {val aType = BooleanType} 

/* Bound Predicates */
abstract class BoundPredicate
case class AttributeEqualityPredicate(attributeName: String, value: BoundValue) extends BoundPredicate

