package deploylib.ec2

import deploylib._
import java.io.File
import net.lag.logging.Logger
import org.jets3t.service.model.S3Object
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.security.AWSCredentials
import org.jets3t.service.acl.AccessControlList

object S3Cache extends AWSConnection {
	protected val logger = Logger()
	protected val credentials = new AWSCredentials(accessKeyId, secretAccessKey)
	val s3Service = new RestS3Service(credentials)
  val bucketName =
    Config.getString("deploylib.aws.s3_cache_bucket", "deploylibCache-" + System.getProperty("user.name"))
	val bucket = s3Service.createBucket(bucketName)
	val md5Cache = new scala.collection.mutable.HashMap[String, String]

	def getCacheUrl(file: File): String = {
		val fileMd5 = Util.md5(file)
		synchronized {
			md5Cache.get(fileMd5) match {
				case Some(url) => url
				case None => {
          val existingObject: Option[S3Object] = try Some(s3Service.getObjectDetails(bucket, fileMd5)) catch {
            case _: org.jets3t.service.S3ServiceException => None
          }

          if(! existingObject.map(_.getMd5HashAsHex == fileMd5).getOrElse(false)) {
            logger.info("Uploading " + file + " to S3")
            val obj = new S3Object(file)
            obj.setKey(fileMd5)
            obj.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ)

            s3Service.putObject(bucket, obj)
          }
					val url =  "http://s3.amazonaws.com/" + bucketName + "/" + fileMd5
					md5Cache.put(fileMd5, url)
					url
				}
			}
		}
	}
}
