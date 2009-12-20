package deploylib.xresults

import scala.xml.Elem

class Histogram(bucketCount: Int, bucketSize: Int) {
	val buckets = new Array[Long](bucketCount)

	def add(value: Int): Unit = {
		val bucket = value / bucketSize

		if(bucket >= bucketCount)
			buckets(bucketCount - 1) += 1
		else
			buckets(bucket) += 1
	}

	def toXml(): Elem = {
		<histogram bucketCount={bucketCount.toString} bucketSize={bucketSize.toString}>
			{buckets.zip((1 to bucketCount - 1).map(_ * bucketSize).toArray ++ Array((bucketCount * bucketSize).toString + "+")).map(b => <bucket max={b._2.toString}>{b._1.toString}</bucket>)}
		</histogram>
	}
}
