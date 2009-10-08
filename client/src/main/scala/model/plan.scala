package edu.berkeley.cs.scads.model.parser

abstract sealed class Plan
abstract class EntityProvider extends Plan
case class Materialize(entityType: String, child: TupleProvider) extends EntityProvider

abstract class TupleProvider extends Plan
case class AttributeKeyedIndexGet(index: AttributeKeyedIndex, values: List[BoundValue]) extends TupleProvider
