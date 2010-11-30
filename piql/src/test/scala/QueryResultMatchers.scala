package edu.berkeley.cs.scads
package piql
package test

import scala.collection.JavaConversions._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

import org.apache.avro.generic.{GenericData, IndexedRecord}

import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

import scala.collection.mutable.{ ArrayBuffer, HashMap }

trait QueryResultMatchers {
  type Tuple = Array[GenericData.Record]
  type QueryResult = Seq[Tuple]

  class QueryResultMatcher[A <: IndexedRecord](right: Seq[Array[A]]) extends Matcher[QueryResult] {
    def apply(left: QueryResult): MatchResult = {
      left.zip(right).foreach {
        case (leftTuple, rightTuple) => {
          leftTuple.zip(rightTuple).foreach {
            case (leftRec, rightRec) => {
              leftRec.getSchema.getFields.foreach(field => {
                val rightVal = rightRec.get(field.pos)
                val leftVal = leftRec.get(field.pos)

                if(rightVal != leftVal) {

                  val string = "%s != %s".format(leftRec, rightRec)
                  return MatchResult(false, string, string)
                }
              })
            }
          }
        }
      }
      return MatchResult(true, "==", "==")
    }
  }

  def returnTuples[A <: IndexedRecord](right: Array[A]) = new QueryResultMatcher(List(right))
  def returnTuples[A <: IndexedRecord](right: Seq[Array[A]]) = new QueryResultMatcher(right)
}
