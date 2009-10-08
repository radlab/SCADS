package edu.berkeley.cs.scads.model.parser

abstract sealed class Plan
case class AttributeKeyedIndexGet(index: AttributeKeyedIndex, values: List[FixedValue]) extends Plan
