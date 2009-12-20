package deploylib.ec2

import java.io.File

import org.apache.log4j.Logger

import org.jets3t.service.model.S3Object
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.security.AWSCredentials
import org.jets3t.service.acl.AccessControlList

object S3Cache extends AWSConnection {
	protected val logger = Logger.getLogger("deploylib.ec2.S3Cache")
	protected val credentials = new AWSCredentials(accessKeyId, secretAccessKey)
	val s3Service = new RestS3Service(credentials)
	val bucket = s3Service.getBucket("deploylibcache")
	val md5Cache = new scala.collection.mutable.HashMap[String, String]

	def getCacheUrl(file: File): String = {
		val fileMd5 = Util.md5(file)
		synchronized {
			md5Cache.get(fileMd5) match {
				case Some(url) => url
				case None => {
					logger.info("Uploading " + file + " to S3")
					val obj = new S3Object(file)
					obj.setKey(fileMd5)
					obj.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ)

					s3Service.putObject(bucket, obj)
					val url = "http://deploylibcache.s3.amazonaws.com/" + fileMd5
					md5Cache.put(fileMd5, url)
					url
				}
			}
		}
	}
}
