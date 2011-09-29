package deploylib
package mesos

import edu.berkeley.cs.avro.marker.{AvroRecord, AvroUnion}
import edu.berkeley.cs.avro.runtime._
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader}
import java.io.ByteArrayOutputStream
import org.apache.avro.io.{EncoderFactory, DecoderFactory}

sealed trait Message extends AvroUnion

/* Messages for the Remote Experiment Running Daemon. Located here due to limitations in MessageHandler */
sealed trait ClassSource extends AvroUnion
case class ServerSideJar(var path: String) extends AvroRecord with ClassSource
case class S3CachedJar(var url: String) extends AvroRecord with ClassSource

object JvmTask {
  def apply(bytes: Array[Byte]): JvmTask = classOf[JvmTask].newInstance.parse(bytes)
  def apply(task: JvmTask): Array[Byte] = task.toBytes
}

sealed trait JvmTask extends AvroUnion
case class JvmWebAppTask(var warFile: ClassSource, var properties: Map[String, String]) extends AvroRecord with JvmTask
case class JvmMainTask(var classpath: Seq[ClassSource], var mainclass: String, var args: Seq[String], var props: Map[String, String] = Map.empty, var env: Map[String, String] = Map.empty) extends AvroRecord with JvmTask

sealed trait ExperimentOperation extends Message
case class RunExperimentRequest(var processes: Seq[JvmTask]) extends AvroRecord with ExperimentOperation
case class RunExperimentResponse() extends AvroRecord with ExperimentOperation

case class KillTaskRequest(var taskId: String) extends AvroRecord with ExperimentOperation
case class KillTaskResponse() extends AvroRecord with ExperimentOperation
