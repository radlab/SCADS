package deploylib.ec2

/**
 * Abstract class that is used to get the the accessKeyId and the secretAcccess key from the environmental variables <code>AWS_ACCESS_KEY_ID</code> and <code>AWS_SECRET_ACCESS_KEY</code> respectively.
 * It is used by the EC2Instance object and S3Cache.
 */
abstract class AWSConnection {
	protected val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
  protected val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
}
