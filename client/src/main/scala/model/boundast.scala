package edu.berkeley.cs.scads.model.parser

import java.text.ParsePosition
import scala.collection.mutable.HashMap



/* Bound counterparts for some of the AST */
abstract sealed class BoundTree

/* SCADS Spec that has been bound and is ready to be optimized */
case class BoundSpec(entities: HashMap[String, BoundEntity], orphanQueries: HashMap[String, BoundQuery])

/* BoundEntity and any queries that depend on its ThisParameter */
case class BoundEntity(name: String, attributes: HashMap[String, AttributeType], keys: List[String]) {
	/* (target, relationshipName) -> BoundRelationship */
	val relationships = new HashMap[(String, String), BoundRelationship]()
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

  def namespace: String = Namespaces.entity(name)
}

/* Bound Relationship */
abstract sealed class RelationshipSide
object ForeignKeyHolder extends RelationshipSide
object ForeignKeyTarget extends RelationshipSide
case class BoundRelationship(name: String, target: BoundEntity, cardinality: Cardinality, side: RelationshipSide)


/* BoundQuery and FetchTree */
case class BoundQuery(fetchTree: BoundFetch, parameters: List[BoundParameter], range:BoundRange) {var plan: QueryPlan = null}
case class BoundFetch(entity: BoundEntity, child: Option[BoundFetch], relation: Option[BoundRelationship], predicates: List[BoundPredicate], orderField: Option[String], orderDirection: Option[Direction])

abstract sealed class BoundRange
case class BoundLimit(lim: Field, max: Int) extends BoundRange
object BoundUnlimited extends BoundRange

/* Bound Values */
object Unexecutable extends Exception
abstract class BoundValue extends Field {
	val name: String
  val aType:AttributeType

 	def serializeKey(): String = throw Unexecutable
	def deserializeKey(data: String, pos: ParsePosition): Unit = throw Unexecutable

	def serialize(): String = throw Unexecutable
	def deserialize(data: String, pos: ParsePosition): Unit = throw Unexecutable

	override def equals(other: Any) = other match {
		case v: BoundValue => (v.name equals name) && (v.aType == aType)
		case _ => false
	}

  def duplicate: Field = throw Unexecutable
}
case class BoundParameter(name: String, aType: AttributeType) extends BoundValue
case class BoundThisAttribute(name: String, aType: AttributeType) extends BoundValue

/* Bound Predicates */
abstract class BoundPredicate
case class AttributeEqualityPredicate(attributeName: String, value: Field) extends BoundPredicate
