package edu.berkeley.cs.scads.perf

import scala.concurrent.SyncVar

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.mesos._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._
import org.apache.zookeeper.CreateMode

import net.lag.logging.Logger

trait ExperimentPart extends optional.Application {
  implicit val zooRoot = RClusterZoo.root
  val logger = Logger()
}

trait Experiment extends ExperimentPart {
  val baseDir = Config.config("mesos.basePath", "/nfs")
  val mesosMaster = Config.config("mesos.master", "0@localhost:5050")
  val scheduler = ServiceScheduler("IntKeyScaleTest", baseDir, mesosMaster)
  val expRoot = zooRoot.getOrCreate("scads/experiments").createChild("IntKeyScaleExperiment", mode = CreateMode.PERSISTENT_SEQUENTIAL)

  def getExperimentalCluster(clusterSize: Int): ScadsMesosCluster = {
    val cluster = new ScadsMesosCluster(expRoot, scheduler, clusterSize)
    println("Cluster located at: " + cluster.root)
    cluster.blockTillReady
    cluster
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
