/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val input=sc.textFile("input.txt")
    val words=  input.flatMap(line => line.split(" "))
    val result= words.map(x => (x,1)).reduceByKey((x,y)=> x+y)
    //val finalResult= result.collect()
    result.saveAsTextFile("output")
    sc.stop()
  }
}

rm -rf target project output 
sbt package
spark-submit --master local[4] target/scala-2.11/simple-project_2.11-1.0.jar
