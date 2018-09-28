import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Lab1_A {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()

    conf.setAppName("Aigul Khannanova")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val file = sc.textFile("C:\\Users\\Бонифация\\IdeaProjects\\SparkTest\\src\\main\\resources\\text.csv") //RDD

    val wordRDD = file.flatMap(line => line.split(" "))

    val wordRDD1 = wordRDD.map(line => (line, 1)).reduceByKey((a, b) => a + b)

    wordRDD1.foreach(println)
  }
}

