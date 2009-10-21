import java.io._
import scala.io._

val dir = "/Users/bodikp/workspace/data/wikipedia/simplified_filtered"
val allFiles = new File(dir).listFiles.toList.sort(_.getName<_.getName)

val N = 100

val firstHour = scala.collection.mutable.Map[String,Int]()
for (line <- Source.fromFile(allFiles.head).getLines) { val s=line.trim.split(","); firstHour += s(0)->s(1).toInt }
val bucketSize = Math.ceil( firstHour.values.reduceLeft(_+_)/N )
val pages = firstHour.keySet.toList.sort(_<_)
var s=0; val cumsum = (for (i<-pages.map(firstHour(_))) yield {s+=i;s}).toList

val splits = ((for (i<-1 to (N-1)) yield { pages(cumsum.zipWithIndex.find(_._1>i*bucketSize).get._2) }).toList+pages.last).zipWithIndex
println("hour,"+splits.map(s=>s._2+"->"+s._1).mkString(","))

for (file <- allFiles) {
	val counts = scala.collection.mutable.Map[Int,Int]()
	for (line <- Source.fromFile(file).getLines) { val s=line.trim.split(","); val i=splits.find(s(0)<=_._1).get._2; counts(i)=counts.getOrElse(i,0)+s(1).toInt }
	println(file.getName+","+splits.map(s=>counts(s._2)).mkString(","))
}
