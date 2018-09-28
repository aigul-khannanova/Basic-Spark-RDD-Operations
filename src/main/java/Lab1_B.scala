import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Lab1_B {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()

    conf.setAppName("Aigul Khannanova")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)


    val file = sc.textFile("C:\\Users\\Бонифация\\IdeaProjects\\SparkTest\\src\\main\\resources\\input.txt")//RDD[string]
    val row=file.map(s=>s.split(" "))
    val numbers = row.map(s=>s.map(t=>t.toInt))
    numbers.map(x=>x.mkString(" ")).foreach(println)

    val SumRows=numbers.map(x=>x.reduce((a,b)=>a+b))
    println("sum of the numbers in each row of file = ")
    SumRows.foreach(println)

    val SumM5=numbers.map(x=>x.filter(a=>(a%5==0)).reduce((b,c)=>b+c))
    println("sum of numbers of each row, which are multiples of the 5 = ")
    SumM5.foreach(println)

    val MaxR=numbers.map(x=>x.max)
    println("max in each row = ")
    MaxR.foreach(println)

    val MinR=numbers.map(x=>x.min)
    println("min in each row = ")
    MinR.foreach(println)

    val DistinctR=numbers.map(x=>x.distinct)
    println("set of distinct numbers in each row = ")
    DistinctR.map(x=>x.mkString(" ")).foreach(println)



    val RDDword=file.flatMap(line=>line.split(' ')).map(x=>x.toInt)

    val S=RDDword.reduce((x,y)=>x+y)
    println("Sum = ",S)

    val SumMOf5=RDDword.filter(x=>(x%5==0)).reduce((x, y)=>x+y)
    println("Multiples of 5 = ",SumMOf5)

    val MaxF=RDDword.max()
    println("Max = ",MaxF)

    val MinF=RDDword.min()
    println("Min = ",MinF)

    val DistF=RDDword.distinct()
    DistF.foreach(println)

  }

}
