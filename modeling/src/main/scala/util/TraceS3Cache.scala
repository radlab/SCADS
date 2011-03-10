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
   protected val s3Credentials = new AWSCredentials(accessKeyId, secretAccessKey) // your credentials here
  val s3Service = new RestS3Service(s3Credentials)
  val bucketName = "piql-modeling-marmbrus" // make this your bucket name
  val bucket = s3Service.createBucket(bucketName)

  def uploadFile(file: File, prefix: String = "", suffix: String = "") {
    val obj = new S3Object(file)
    obj.setKey(prefix + "/" + file.getName() + "-" + suffix + "-" + System.currentTimeMillis)
    obj.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ)
    s3Service.putObject(bucket, obj)
  }
}
