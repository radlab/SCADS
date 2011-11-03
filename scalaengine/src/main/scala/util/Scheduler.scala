package edu.berkeley.cs.scads.util

import compat.Platform
import java.util.concurrent.{TimeUnit, Executors}

object Scheduler {
    private lazy val sched = Executors.newSingleThreadScheduledExecutor();
    def schedule(f: () => Unit, time : Long) {
        sched.schedule(new Runnable {
          def run = {
            f()
            //actors.Scheduler.execute(f)
          }
        }, time, TimeUnit.MILLISECONDS);
    }
}