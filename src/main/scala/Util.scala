package deploylib

import java.io.File
import java.security.MessageDigest
import java.math.BigInteger
import java.io.FileInputStream
import org.apache.log4j.Logger

object Util {
	val logger = Logger.getLogger("deploylib.util")

	def retry[ReturnType](tries: Int)(func: () => ReturnType):ReturnType = {
		var usedTries = 0
		var lastException: Exception = null

		while(usedTries < tries) {
			usedTries += 1
			try {
				return func()
			}
			catch {
				case t: org.apache.thrift.transport.TTransportException => {
					lastException = t
					logger.warn("Retrying due to thrift failure " + t + ": " + usedTries + " of " + tries)
				}
			}
		}
		throw lastException
	}

	def username: String = {
		if(System.getenv("DEPLOY_USER") != null)
			return System.getenv("DEPLOY_USER")
		else if(System.getProperty("deploy.user") != null)
			return System.getProperty("deploy.user")
		else
			return System.getProperty("user.name")
	}

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
}
