package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Spec}
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.avro.runtime.SchemaCompare
import org.apache.avro.Schema

@RunWith(classOf[JUnitRunner])
class SchemaCompareSpec extends Spec {
  
  val s1 = Schema.parse(
    """
    { "type": "record", "name": "aaa", "fields": [
      {"name":"fefee", "type":"int"},
      {"name":"xxxte", "type":"float"}] }
    """
  )

  val s15 = Schema.parse(
    """
    { "type": "record", "name": "aaa", "fields": [
      {"name":"fefee", "type":"int"}] }
    """
  )

  val s2 = Schema.parse(
    """
    { "type": "record", "name": "xxx", "fields": [
      {"name":"ifee", "type":"int"},
      {"name":"ite", "type":"float"}] }
    """
  )

  val s3 = Schema.parse(
    """
    { "type": "record", "name": "xxx", "fields": [
      {"name":"ifee", "type":"int"},
      {"name":"ite", "type":"double"}] }
    """
  )
    
  val s4 = Schema.parse(
    """{"type": "record", "name": "efwefew", "fields": [
      {"name":"id", "type":"int"},
      {"name":"f", "type":{"type":"record", "name":"rec", "fields": [
        {"name":"ifefe", "type":["string","int"]},
        {"name":"m1", "type":{"type":"map", "values":"long"}},
        {"name":"blah","type":{"type":"array","items":"string"}}
      ]}},
      {"name":"e1", "type":{"type": "enum", "name": "Suit", "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}},
      {"name":"f1", "type":{"type":"fixed","name":"fewfe","size":23}},
      {"name":"okefew", "type":"bytes"}]}"""
  )

  val s5 = Schema.parse(
    """{"type": "record", "name": "cwefewre", "fields": [
      {"name":"iwefew", "type":"int"},
      {"name":"ffewf", "type":{"type":"record", "name":"ffrec", "fields": [
        {"name":"iffefe", "type":["string","int"]},
        {"name":"m2", "type":{"type":"map", "values":"long"}},
        {"name":"blddah","type":{"type":"array","items":"string"}}
      ]}},
      {"name":"e2", "type":{"type": "enum", "name": "Suiwef", "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}},
      {"name":"f2", "type":{"type":"fixed","name":"ijefe","size":23}},
      {"name":"okefddew", "type":"bytes"}]}"""
  )

  val s6 = Schema.parse(
    """{"type": "record", "name": "cwefewre", "fields": [
      {"name":"iwefew", "type":"int"},
      {"name":"ffewf", "type":{"type":"record", "name":"ffrec", "fields": [
        {"name":"iffefe", "type":["string","int"]},
        {"name":"m2", "type":{"type":"map", "values":"long"}},
        {"name":"blddah","type":{"type":"array","items":"string"}}
      ]}},
      {"name":"e2", "type":{"type": "enum", "name": "Suiwef", "symbols" : ["SPADES", "HEARTS", "DIMONDS", "CLUBS"]}},
      {"name":"f2", "type":{"type":"fixed","name":"ijefe","size":23}},
      {"name":"okefddew", "type":"bytes"}]}"""
  )

  describe("SchemaCompare") {
    it("should say two simple schemas are equal") {
      assert(SchemaCompare.typesEqual(s1,s2))
    }

    it("should say schema that is prefix of another are not equals") {
      assert(!SchemaCompare.typesEqual(s1,s15))
      assert(!SchemaCompare.typesEqual(s15,s1))
    }

    it("should say two different simple schema are not equal") {
      assert(!SchemaCompare.typesEqual(s1,s3))
    }

    it("should say a simple schema does not equal a complex one") {
      assert(!SchemaCompare.typesEqual(s1,s4))
    }

    it("should say two complex equal type schemas are equal") {
      assert(SchemaCompare.typesEqual(s4,s5))
    }

    it("should say two different complex schema are not equal") {
      assert(!SchemaCompare.typesEqual(s4,s6))
    }

  }
}
