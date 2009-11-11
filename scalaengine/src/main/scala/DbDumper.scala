package edu.berkeley.cs.scads

import java.io.File
import scala.collection.jcl.Conversions.convertList
import com.sleepycat.je.{Environment, EnvironmentConfig, DatabaseEntry, OperationStatus}

object DbDumper {
	def main(args: Array[String]): Unit = {
		if(args.size != 1){
			println("Usage: DbDumper <db env dir>")
			System.exit(1)
		}

		val envDir = new File(args(0))
		val config = new EnvironmentConfig()
		val env = new Environment(envDir, config)
		val key = new DatabaseEntry
		val value = new DatabaseEntry

		env.getDatabaseNames.foreach(dbName => {
			val db = env.openDatabase(null, dbName, null)
			val cur = db.openCursor(null, null)

			println("====" + dbName + "====")

			var status = cur.getFirst(key, value, null)
			while(status != OperationStatus.NOTFOUND) {
				println("key:")
				println(new String(key.getData))
				println("value:")
				println(new String(value.getData))
				status = cur.getNext(key, value, null)
			}
		})
	}
}
