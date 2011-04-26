package edu.berkeley.cs
package scads
package axer

import avro.marker.{ AvroRecord, AvroUnion, AvroPair }

sealed trait RequestAction extends AvroUnion

case class Get(var key: String)
  extends AvroRecord
  with    RequestAction

case class Request(var from: String, var actions: List[RequestAction])
  extends AvroRecord
