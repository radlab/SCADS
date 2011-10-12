package edu.berkeley.cs
package scads
package piql
package modeling

import storage._
import avro.runtime._
import avro.marker._
import comm.ServiceId

/* Schema for PIQL modeling */
case class ExecutionTrace(var timestamp: Long, var thread: String, var event: TraceEvent) extends AvroRecord

sealed trait TraceEvent extends AvroUnion
case class QueryEvent(var queryName: String, var params: Seq[Int], var queryCounter: Int, var start: Boolean) extends AvroRecord with TraceEvent
case class IteratorEvent(var iteratorName: String, var planId: Int, var operation: String, var start: Boolean) extends AvroRecord with TraceEvent
case class MessageEvent(var message: Array[Byte]) extends AvroRecord with TraceEvent
case class Envelope(var src: ServiceId, var dest: ServiceId, var msg: StorageMessage) extends AvroRecord

// messages for varying query params - used during data collection to facilitate log parsing
case class ChangeCardinalityEvent(var numDataItems: Int) extends AvroRecord with TraceEvent
case class WarmupEvent(var warmupLengthInMinutes: Int, var start: Boolean) extends AvroRecord with TraceEvent	// to avoid JIT effects on latency measurements

case class ChangeNamedCardinalityEvent(var nameOfCardinality: String, var numDataItems: Int) extends AvroRecord with TraceEvent
