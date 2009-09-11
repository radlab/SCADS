package edu.berkeley.cs.scads.model.parser

sealed abstract class Tree

/* Entities */
case class Attribute(name: String, fieldType: String) extends Tree
case class Entity(name: String, attributes: List[Attribute], keys: List[String]) extends Tree

/* Relationships */
abstract class Cardinality extends Tree
object OneCardinality extends Cardinality
case class FixedCardinality(max: Int) extends Cardinality
object InfiniteCardinality extends Cardinality

case class Relationship(name: String, from: String, to: String, cardinality: Cardinality) extends Tree

/* Queries */
abstract class Value extends Tree
case class Parameter(name: String, ordinal: Int) extends Value
object ThisParameter extends Value
case class StringValue(value: String) extends Value
case class NumberValue(num: Integer) extends Value
case class Field(entity: String, name: String) extends Value
object TrueValue extends Value
object FalseValue extends Value

abstract class Predicate extends Tree
case class EqualityPredicate(op1: Value, op2: Value) extends Predicate

case class Join(entity: String, relationship: String, alias: String) extends Tree

case class Query(name: String, fetches: List[Fetch]) extends Tree

case class Fetch(
	returnEntity: String,
	joins: List[Join],
	predicates: List[Predicate],
	order: Order,
	range: Range) extends Tree

abstract class Range extends Tree
case class Limit(lim: Value, max: Int) extends Range
case class OffsetLimit(lim: Value, max: Int, offset: Value) extends Range
case class Paginate(perPage: Value, max: Int) extends Range
object Unlimited extends Range

abstract class Order extends Tree
object Unordered extends Order
case class OrderedByField(fields: List[Field]) extends Order

/* SCADS Spec */
case class Spec(entities: List[Entity], relationships: List[Relationship], queries: List[Query]) extends Tree
