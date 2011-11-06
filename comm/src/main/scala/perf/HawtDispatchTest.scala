package perf
import _root_.org.fusesource.hawtdispatch._
import java.util.concurrent.Semaphore


/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: 11/4/11
 * Time: 12:39 PM
 * To change this template use File | Settings | File Templates.
 */

object HawtDispatchTest {
  def main(args: Array[String]): Unit = {
    val done = new Semaphore(1 - (1000 * 1000))

    val queue = createQueue()
    val source = createSource(EventAggregators.INTEGER_ADD, queue)
    source.onEvent {
      val count = source.getData()
      println("got: " + count)
      done.release(count.intValue)
    }
    source.resume();

    // Produce 1,000,000 concurrent merge events
    for (i <- 0 until 1000) {
        for (j <- 0 until 1000) {
          source.merge(1)
        }
    }

    // Wait for all the event to arrive.
    done.acquire()
  }
}