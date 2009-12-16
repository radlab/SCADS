package deploylib.ec2

abstract class AWSConnection {
	protected val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
  protected val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
}
