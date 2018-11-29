import org.apache.spark.sql.SparkSession
//import spark._

object CoreTest {
  def main(args:Array[String]): Unit ={
    val spark=SparkSession.builder().master("local").appName("coreTest").getOrCreate()
//    spark.conf.set("spark.executor.memory","2g")
    //spark.conf.getAll.foreach(println)
    import spark.implicits._
    val test=spark.range(5,100,5)
    //test.orderBy("id")
    //D:\data\graphx-wiki-edges.txt
    //D:\data\graphx-wiki-vertices.txt
    val vertices=spark.read.text("file:///D:\\data\\graphx-wiki-vertices.txt")
//    val vertices2=spark.read.csv("file:///D:\\data\\graphx-wiki-vertices.txt")
    val edges=spark.read.textFile("file:///D:\\data\\graphx-wiki-edges.txt")

//    import spark.implicits._
//    val vertices2=vertices.map(e=>(e.toString().split('\t')(0),e.toString().split('\t')(1)))
//    val vertices2=vertices.map(e=>e.get(0))
//      pr
//    println(vertices.count())
//    vertices.limit(100).show(false)
//    vertices.describe("value") .show(false)
//    vertices.explode("value","value2"){in:String=>in.split("\t")}.show(100,0)
//    vertices.registerTempTable()
//    vertices2.take(100).foreach(println)
//    vertices.select("value").map(e=>(e.get(0)))
//    vertices.map(_.toSeq).show(100,0)
    import org.apache.spark.sql.functions._
    vertices.withColumn("value2",split($"value","\t")(0)).show(100,0)
//    edges.collect().take(100).foreach(println)
//    vertices.collect().take(100).foreach(println)
  }

}
