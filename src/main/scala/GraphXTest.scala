import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphXTest {

  def getGrahp(sks:SparkSession): Graph[(String,String),Int] ={
    var vertexArray=Array(
      (1L,("a","a")),
      (2L,("a","a")),
      (3L,("a","a")),
      (7L,("c","c")),
      (8L,("c","c")),
      (4L,("a","a")),
      (5L,("b","b")),
      (6L,("c","c")),
      (9L,("c","c")),
      (10L,("c","c")),
      (11L,("c","c")),
      (12L,("c","c"))
    )
    val edgeArray=Array(
      Edge(1L,2L,5),
      Edge(1L,3L,9),
      Edge(1L,8L,9),
      Edge(4L,2L,5),
      Edge(4L,3L,9),
      Edge(5L,3L,10),
      Edge(5L,6L,10),
      Edge(3L,9L,10),
      Edge(2L,10L,10),
      Edge(1L,5L,10),
      Edge(1L,6L,10),
      Edge(2L,11L,10),
      Edge(3L,12L,10),
      Edge(5L,1L,10)
    )

    val vertexRDD:RDD[(Long,(String,String))]=sks.sparkContext.parallelize(vertexArray)
    val edgeRDD:RDD[Edge[Int]]=sks.sparkContext.parallelize(edgeArray)

    val graph:Graph[(String,String),Int]=Graph(vertexRDD,edgeRDD)

    return graph

  }
  def main(args:Array[String]): Unit ={
    val sks=SparkSession.builder()
      .master("local")
      .appName("graphxTest")
      .getOrCreate()

    val graph=getGrahp(sks)

//    graph.vertices.collect().foreach(println)
  }
}
