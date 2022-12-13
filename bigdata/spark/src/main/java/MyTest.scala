import org.apache.spark.{SparkConf, SparkContext}

class MyTest {
  def main(args: Array[String]): Unit = {
    println(123)
    val conf = new SparkConf().setAppName("firstapp")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(List(1, 2, 3, 4))
    a.persist();
    println(a.count())
    println("============================")
    a.collect().foreach(println)
  }
}
