package edu.berkeley.cs.scads.model.parser

abstract sealed class Plan
case class PrimaryKeyGet(entityType: String, values: List[FixedValue]) extends Plan
