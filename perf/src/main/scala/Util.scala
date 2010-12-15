package edu.berkeley.cs.scads
package perf

import scala.concurrent.SyncVar

import comm._
import deploylib._
import deploylib.config._
import deploylib.mesos._
import deploylib.ec2._
import deploylib.rcluster._

import java.io.File

object Deploy extends ConfigurationActions {
  implicit def toFile(str: String) = new java.io.File(str)

  def classpath = System.getProperty("java.class.path").split(":")
  def s3Classpath = classpath.map(f => S3CachedJar(S3Cache.getCacheUrl(new File(f)))).toSeq
  def codeS3Classpath = s3Classpath.map(j => """S3CachedJar("%s")""".format(j.url)).toList.toString

  def workClasspath = {
    classpath.map(jar => {
      val cacheLocation = r2.cacheFile(jar)
      ServerSideJar(cacheLocation.getCanonicalPath)
    }).toSeq
  }

  def deployJars: Unit = {
    EC2Instance.activeInstances.pforeach(i => i.upload("target/perf-2.1.0-SNAPSHOT-jar-with-dependencies.jar", "/root"))
    EC2Instance.activeInstances.pforeach(i => i.upload("target/perf-2.1.0-SNAPSHOT.jar", "/root"))
    EC2Instance.activeInstances.pforeach(i => i.upload("setup.scala", "/root"))
  }

  def deployCurrentClassPath: Unit = {
    val localSetupFile = Util.readFile("setup.scala")
    val remoteFileLines = localSetupFile.split("\n") ++
      ("implicit val classpath = %s".format(codeS3Classpath) ::
       "implicit val scheduler = LocalExperimentScheduler(\"MasterConsole\")" ::
       "implicit val zookeeper = ZooKeeperNode(\"zk://mesos-ec2.knowsql.org/\")" :: Nil)

    MesosEC2.master.upload("target/perf-2.1.0-SNAPSHOT-jar-with-dependencies.jar", "/root")
    MesosEC2.master.upload("target/perf-2.1.0-SNAPSHOT.jar", "/root")
    createFile(MesosEC2.master, "/root/setup.scala", remoteFileLines.mkString("\n"), "644")
  }
}

object Future {
	def apply[A](f: => A): Future[A] = new Future(f)
}

class CanceledException extends Exception

class Future[A](f: => A) {
	val result = new SyncVar[Either[A, Throwable]]
	val thread = new Thread {override def run() = {result.set(tryCatch(f))}}
	thread.setDaemon(true)
	thread.start()

	def cancel():Unit = {
		if(!isDone) {
			thread.interrupt()
			result.set(Right(new CanceledException))
		}
	}

	def isDone: Boolean = {
		result.isSet
	}

	def success: Boolean = {
		result.get match {
			case Left(a) => true
			case Right(t) => false
		}
	}

	def apply(): A = {
		result.get match {
			case Left(a) => a
			case Right(t) => throw t
		}
	}

	private def tryCatch[A](left: => A): Either[A, Throwable] = {
		try {
			Left(left)
		} catch {
			case t => Right(t)
		}
	}

	override def toString(): String = {
		val result = new StringBuilder
		result.append("<Future ")
		if(isDone){
			result.append("completed ")
			if(success)
				result.append("sucessfully")
			else
				result.append("with exception")
		}
		else
			result.append("running")
		result.append(">")
		result.toString()
	}
}

object ParallelConversions {
	implicit def toParallelSeq[A](itr: Iterable[A]): ParallelSeq[A] = new ParallelSeq(itr.toList)
}

class ParallelSeq[A](seq: List[A]) {

  @deprecated("use apmap")
  def spmap[B](f : (A) => B) : List[Future[B]] = apmap(f)

  def apmap[B](f : (A) => B) : List[Future[B]] = {
		seq.map(v => new Future(f(v)))
	}

	def pmap[B](f : (A) => B) : List[B] = {
		seq.map(v => new Future(f(v))).map(_())
	}

	def pflatMap[B](f : (A) => Iterable[B]) : List[B] = {
		seq.map(v => new Future(f(v))).flatMap(_())
	}

	def pforeach(f : (A) => Unit) : Unit = {
		seq.map(v => new Future(f(v))).map(_())
	}
}
