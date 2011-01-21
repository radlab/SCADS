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

  protected val credentials = new AWSCredentials(accessKeyId, secretAccessKey) // your credentials here
  val s3Service = new RestS3Service(credentials)
  val bucketName = "mybucket" // make this your bucket name
  val bucket = s3Service.createBucket(bucketName)

  def uploadFile(file: File) {
    val obj = new S3Object(file)
    obj.setKey("myfile") // specify filename somewhere
    obj.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ)
    s3Service.putObject(bucket, obj)
  }
}
