package deploylib

import scala.concurrent.SyncVar

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

  def pfilter(f : (A) => Boolean) : List[A] = {
    seq.map(v => new Future((v, f(v)))).map(_()).filter(_._2).map(_._1)
  }
}
