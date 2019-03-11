import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphXTest {

  def getGrahp(sks:SparkSession): Graph[(String,String),Int] ={

    var vertexArray=Array(
      (1L,("a","a")),
      (2L,("a","a")),
      (3L,("a","a")),
      (7L,("c","c")),
      (8L,("c","t")),
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

    val vertexRDD:RDD[(VertexId,(String,String))]=sks.sparkContext.parallelize(vertexArray)
    val edgeRDD:RDD[Edge[Int]]=sks.sparkContext.parallelize(edgeArray)

    val graph:Graph[(String,String),Int]=Graph(vertexRDD,edgeRDD)

    return graph
  }

  def graphxBasic(graph:Graph[(String,String),Int]): Unit ={
    graph.mapVertices((a,b)=>(b._2,b._1)).vertices.collect().foreach(println)

  }
  def vertciesTools(vertices:VertexRDD[(String,String)]): Unit ={
    vertices.collect.foreach(println(_))

    // 使用filter筛选出属性value>150的顶点
    val filteredVertices:VertexRDD[(String, String)] =
      vertices.filter{ case (vid:VertexId, (name:String, value:String)) => name=="c" }
    filteredVertices.collect.foreach(println(_))

    // 除了mapVertices之外，也可以使用VertexRDD的mapValues来修改顶点的属性
    val mappedVertices:VertexRDD[String] =
      vertices.mapValues((vid:VertexId, attr:(String, String)) => attr._1)

    //diff(other: RDD[(VertexId, VD)]): VertexRDD[VD]
    //For each vertex present in both this and other, diff returns only those vertices with differing values;
    // for values that are different, keeps the values from other.
    val filteredVertices2=filteredVertices.mapValues(line=>("d","d"))

    val diffedVertices:VertexRDD[(String, String)] = vertices.diff(filteredVertices2)
    diffedVertices.collect().foreach(println(_))
    println("vertices : " + vertices.count)
    println("filteredVertices : " + filteredVertices2.count)
    println("diffedVertices : " + diffedVertices.count)









  }

  def edgesTools(edges:EdgeRDD[Int]){
    edges.collect.foreach(println(_))
  }

  def main(args:Array[String]): Unit ={
    val sks=SparkSession.builder()
      .master("local")
      .appName("graphxTest")
      .getOrCreate()

    val graph=getGrahp(sks)

    graphxBasic(graph)

    //    属性的操作
    //    结构的操作
    //    链接操作

    val vertices=graph.vertices
//    vertciesTools(vertices)

    val edges=graph.edges
//    edgesTools(edges)

    //graph.vertices.collect().foreach(println)

  }

}
