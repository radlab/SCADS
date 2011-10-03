package edu.berkeley.cs
package scads

import avro.runtime._

package object storage {
  implicit object StorageRegistry extends comm.ServiceRegistry[StorageMessage]
}