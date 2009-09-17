package edu.berkeley.cs.scads.model.parser

import org.apache.log4j.Logger

/* Exceptions that can occur during binding */
class BindingException extends Exception
case class DuplicateEntityException(entityName: String) extends BindingException
case class DuplicateAttributeException(entityName: String, attributeName: String) extends BindingException
case class DuplicateRelationException(relationName: String) extends BindingException
case class UnknownEntityException(entityName: String) extends BindingException
case class DuplicateQueryException(queryName: String) extends BindingException
case class DuplicateParameterException(queryName: String) extends BindingException
case class BadParameterOrdinals(queryName:String) extends BindingException

/* Bound counterparts for some of the AST */
case class BoundRelationship(target: String, cardinality: Cardinality)
case class BoundEntity(attributes: scala.collection.mutable.HashMap[String, AttributeType], keys: List[String]) {
	val relationships = new scala.collection.mutable.HashMap[String, BoundRelationship]()
	val queries = new scala.collection.mutable.HashMap[String, BoundQuery]()

	def this(e: Entity) {
		this(new scala.collection.mutable.HashMap[String, AttributeType](), e.keys)

		e.attributes.foreach((a) => {
			attributes.get(a.name) match {
				case Some(_) => throw new DuplicateAttributeException(e.name, a.name)
				case None => this.attributes.put(a.name, a.attrType)
			}
		})
	}
}

case class BoundQuery

object Binder {
	val logger = Logger.getLogger("scads.binding")

	def bind(spec: Spec) {
		/* Bind entities into a map and check for duplicate names */
		val entityMap = new scala.collection.mutable.HashMap[String, BoundEntity]()
		spec.entities.foreach((e) => {
			entityMap.get(e.name) match
			{
				case Some(_) => throw new DuplicateEntityException(e.name)
				case None => entityMap.put(e.name, new BoundEntity(e))
			}
		})

		/* Bind relationships to the entities they link, check for bad entity names and duplicate relationship names */
		spec.relationships.foreach((r) => {
			entityMap.get(r.from) match {
				case None => throw new UnknownEntityException(r.from)
				case Some(entity) => entity.relationships.put(r.name, new BoundRelationship(r.to, r.cardinality))
			}

			entityMap.get(r.to) match {
				case None => throw new UnknownEntityException(r.to)
				case Some(entity) => entity.relationships.put(r.name, new BoundRelationship(r.from, r.cardinality))
			}
		})

		val orphanQueryMap = new scala.collection.mutable.HashMap[String, BoundQuery]()
		spec.queries.foreach((q) => {
			/* Extract all Parameters from Predicates */
			val predParameters: List[Parameter] =
				q.fetch.predicates.map(
					_ match {
						case EqualityPredicate(op1, op2) => {
							val p1 = op1 match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
							val p2 = op2 match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
							p1 ++ p2
						}
					}).flatten
			/* Extract possible parameter from the limit clause */
			val limitParameters: Array[Parameter] =
				q.fetch.range match {
					case Limit(p, _) => p match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
					case Paginate(p, _) => p match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
					case Unlimited => Array[Parameter]()
				}
			val parameters = predParameters ++ limitParameters

			/* Ensure any duplicate parameter names are actually the same parameter */
			if(Set(parameters: _*).size != Set(parameters.map(_.name): _*).size)
				throw new DuplicateParameterException(q.name)

			/* Ensure that parameter ordinals are contiguious starting at 1 */
			parameters.sort(_.ordinal > _.ordinal).foldRight(1)((p: Parameter, o: Int) => {
				logger.debug("Ordinal checking, found " + p + " expected " + o)
				if(p.ordinal != o)
					throw new BadParameterOrdinals(q.name)
				o + 1
			})
		})
	}

}
