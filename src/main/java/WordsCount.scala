import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordsCount {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("WordFrequency")
    conf.setMaster("local[2]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    val inputFile = "words.csv"

    // Load our input data.
    val input =  sc.textFile(inputFile)
    // Split up into words.

    val words = input.flatMap(s => s.split(" "))

    var wordsNumbers = words.map(s => (s, 1))
    // Transform into word and count.
    val counts = wordsNumbers.reduceByKey { case (a, b) => a + b }
    // Save the word count back out to a text file, causing evaluation.
    var fullCount = words.count().toFloat

    var wordFrq = counts.map(x=>(x._1,x._2,x._2.toFloat / fullCount))

    counts.foreach(println)
  }
}
