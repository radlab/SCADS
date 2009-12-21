package scaletest

import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig
import edu.berkeley.cs.scads.storage.{StorageProcessor, TestableStorageNode}
import deploylib.xresults._
import deploylib._
import scala.util.Random
import java.io.File

object BdbRaw extends IntKeyTest {
	options.addOption("t", "threads", true, "the number of concurrent read threads")
	options.addOption("k", "keys", true, "the number of keys to read/write")
	options.addOption("r", "repeat", true, "the number of times to repeat each test")
	options.addOption("l", "length", true, "the length of the read test")

	def runExperiment(): Unit = {
		val seed = (XResult.hostname + Thread.currentThread().getName + System.currentTimeMillis).hashCode
		val rand = new Random(seed)

		val maxKey = getIntOption("keys")
		val dbDir = new File("db")
		TestableStorageNode.rmDir(dbDir)
		dbDir.mkdir

		val config = new EnvironmentConfig();
		config.setAllowCreate(true);
		config.setTransactional(true);
		val env = new Environment(dbDir, config)

		val processor = new StorageProcessor(env, true)

		(1 to getIntOption("repeat")).foreach(i => {
			logger.info("Writing " + maxKey + " keys")
			XResult.recordResult(
				XResult.benchmark {
					(1 to getIntOption("keys")).foreach(k => {
						processor.put("intKeys", makeRecord(k))
					})
					<rawSequentialLoad><keys>{getIntOption("keys")}</keys></rawSequentialLoad>
				}
			)

			logger.info("Reading " + maxKey + " keys")
			val readResults = (1 to getIntOption("threads")).toList.map(i => Future {
				XResult.timeLimitBenchmark(getIntOption("length"), 1, <rawRandomReads><keys>{getIntOption("keys")}</keys></rawRandomReads>) {
					val key = rand.nextInt(maxKey) + 1
					processor.get("intKeys", makeKey(key)).value equals ("value" + key)
				}
			})

			XResult.recordResult(readResults.map(r => <thread>{r()}</thread>))
		})
	}
}
