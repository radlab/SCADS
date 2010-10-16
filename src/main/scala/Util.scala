package deploylib

import java.io.File
import java.security.MessageDigest
import java.math.BigInteger
import java.io.FileInputStream
import net.lag.logging.Logger

abstract class RetryableException extends Exception

/**
 * A set of utility functions for reliably running experiments.
 */
object Util {
	val logger = Logger()

	/**
	 * Runs the provided function and will retry, pausing for 1 second, in the case of a number of transient failures (for instance TTransportException).
	 */
	def retry[ReturnType](tries: Int)(func: => ReturnType):ReturnType = {
		var usedTries = 0
		var lastException: Exception = null

		def logAndStore(e: Exception) = {
			lastException = e
			logger.warning("Retrying due to" + e + ": " + usedTries + " of " + tries)
			Thread.sleep(1000)
		}

		while(usedTries < tries) {
			usedTries += 1
			try {
				return func
			}
			catch {
				case ce: java.net.ConnectException => logAndStore(ce)
				case rt: RetryableException => logAndStore(rt)
			}
		}
		throw lastException
	}

	/**
	 * Returns the active username.  Checks (in order) the environmental variable DEPLOY_USER, the system property deploy.user, and finally the system property user.name.
	 */
	def username: String = {
		if(System.getenv("DEPLOY_USER") != null)
			return System.getenv("DEPLOY_USER")
		else if(System.getProperty("deploy.user") != null)
			return System.getProperty("deploy.user")
    else if (Config.getString("deploylib.user").isDefined)
      return Config.getString("deploylib.user").get
		else
			return System.getProperty("user.name")
	}

	/**
	 * Calculates and returns the 32 character hex representation of the md5 hash of a local file.
	 */
	def md5(file: File): String = {
		val digest = MessageDigest.getInstance("MD5");
		val buffer = new Array[Byte](1024*1024)
		val is = new FileInputStream(file)

		var len = is.read(buffer)
		while(len > 0) {
			digest.update(buffer, 0, len)
			len = is.read(buffer)
		}
		val md5sum = digest.digest()
		val bigInt = new BigInteger(1, md5sum)
		var bigIntStr = bigInt.toString(16)
        // pad to 32 length string
        while ( bigIntStr.length < 32 ) {
            bigIntStr = "0" + bigIntStr
        }
        bigIntStr
	}

	/**
	 * Reads in the given file an returns it as a string
	 */
	def readFile(file: File): String = {
		val fis = new java.io.FileInputStream(file)
		val in = new java.io.BufferedReader(new java.io.InputStreamReader(fis, "UTF-8"))
		val ret = new StringBuilder
		var line: String = in.readLine()

		while(line != null) {
			ret.append(line)
			ret.append("\n")
			line = in.readLine()
		}

		return ret.toString()
	}
}
