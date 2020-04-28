/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner

object PageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val lines=sc.textFile("input.txt").cache().filter(!_.isEmpty())

    println("How many iterations?")
    var itr = scala.io.StdIn.readInt()
   val links = lines.map(x => (x.split(" ")(0).toString, x(2).toString))
                     .groupByKey()
                     .persist()

val numNodes=  lines.flatMap(line => line.split(" ")).distinct.count


var ranks = links.mapValues(v => (1.0/numNodes))

// Run 10 iterations of PageRank
    //fist pass, gives us missing mass

for (i <- 0 until itr) {
 //first pass, gives us missing mass
  val oldValue = links.join(ranks).flatMap {
    case (pageId, (pageLinks, rank)) =>
      pageLinks.map(dest => (dest, rank / pageLinks.size))
  }

    //second pass, gives us current mass
  val currValue = links.join(oldValue.distinct()).flatMap {
    case (pageId, (pageLinks, rank)) =>
      pageLinks.map(dest => (dest, rank / pageLinks.size))
  }

 ranks = oldValue.join(currValue).distinct().flatMap{ 
    case (pageId, (oldValue, newValue)) =>
      pageId.map(dest => (dest.toString, (0.1/numNodes)+((0.9*(oldValue/numNodes))+newValue) ))
  }

}

ranks.saveAsTextFile("output")
 sc.stop()
  }
}

