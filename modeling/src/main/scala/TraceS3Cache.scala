package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib._
import deploylib.ec2._
import java.io.File
import net.lag.logging.Logger
import org.jets3t.service.model.S3Object
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.security.AWSCredentials
import org.jets3t.service.acl.AccessControlList

object TraceS3Cache extends AWSConnection {

  protected val credentials = new AWSCredentials("AKIAILGVHXBVDZKFJZQQ", "VdB4xNttSvG8DOeF90XQI4jqg6EOi6L00nt0Lq3n") // your credentials here
  val s3Service = new RestS3Service(credentials)
  val bucketName = "piql-modeling" // make this your bucket name
  val bucket = s3Service.createBucket(bucketName)

  def uploadFile(file: File, suffix: String = "") {
    val obj = new S3Object(file)
    obj.setKey(file.getName() + "-" + suffix)
    obj.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ)
    s3Service.putObject(bucket, obj)
  }
}
