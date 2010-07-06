package edu.berkeley.cs.scads.piql.parser

sealed abstract class Tree

/* Entities */
abstract class AttributeType extends Tree
object BooleanType extends AttributeType
object StringType extends AttributeType
object IntegerType extends AttributeType

case class Entity(name: String, attributes: List[Attribute], keys: List[String]) extends Tree

abstract class Attribute {val name: String}
case class ForeignKey(name: String, foreignType: String, cardinality: Cardinality) extends Attribute
case class SimpleAttribute(name: String, attrType: AttributeType) extends Attribute
case class NullableAttribute(name: String, attrType: AttributeType) extends Attribute

/* Relationship Cardinalities */
abstract class Cardinality extends Tree
case class FixedCardinality(max: Int) extends Cardinality
object InfiniteCardinality extends Cardinality

/* Queries */
abstract class Value extends Tree
case class AttributeValue(entity: String, name: String) extends Value

abstract class FixedValue extends Value
object ThisParameter extends FixedValue
case class Parameter(name: String, ordinal: Int) extends FixedValue

abstract class Literal extends FixedValue
case class StringValue(value: String) extends Literal
case class NumberValue(num: Int) extends Literal
object TrueValue extends Literal
object FalseValue extends Literal

abstract class Predicate extends Tree
case class EqualityPredicate(op1: Value, op2: Value) extends Predicate

case class Join(entity: String, relationship: String, alias: String) extends Tree

case class Query(name: String, joins: List[Join], predicates: List[Predicate], order: Order, range: Range) extends Tree

abstract class Range extends Tree
case class Limit(lim: Value, max: Int) extends Range
case class Paginate(perPage: Value, max: Int) extends Range
object Unlimited extends Range

abstract class Order extends Tree
object Unordered extends Order
case class OrderedByField(fields: AttributeValue, direction: Direction) extends Order

abstract class Direction extends Tree
object Ascending extends Direction
object Descending extends Direction

/* SCADS Spec */
case class Spec(entities: List[Entity], queries: List[Query]) extends Tree
