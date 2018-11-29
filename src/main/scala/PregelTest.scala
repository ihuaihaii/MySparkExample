import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object PregelTest {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    //println("ssssssssssssssssssssssssss")

    var vertexArray=Array(
      (1L,(1)),
      (2L,(1)),
      (3L,(1)),
      (4L,(1)),
      (5L,(1)),
      (6L,(1))
    )



    val edgeArray=Array(
      Edge(1L,2L,100),
      Edge(1L,3L,48),
      Edge(1L,4L,100),
      Edge(4L,5L,100),
      Edge(4L,6L,100),
      Edge(2L,1L,52)
    )


    val vertexRDD:RDD[(Long,(Int))]=sc.parallelize(vertexArray)
    val edgeRDD:RDD[Edge[Int]]=sc.parallelize(edgeArray)


    val graph:Graph[(Int),Int]=Graph(vertexRDD,edgeRDD)

    //graph.con

    val graph1=graph.mapVertices((id,attr)=>(Map(id->Map(id->1)),Map(id->Map(id->1))))

    graph1.vertices.filter{case(id,attr)=>id==77897}

    val a=Map("2"->2,"1"->4)

//    println(a)
//
//    println(a.size)
//    a.foreach(println)
//    println(a.toString())
//    println(a.keys)
//    println(a.values)
    val c=a.map(e=>Map(e._1->e._2))
//    c.foreach(println)
//    println(c)
    println(a.map(e=>(e._1,e._2)).values.sum)
    println("11111-----",a.map(e=>(e._1,e._2)))



//    val a=Map("1"->Map("1"->1))
//    val b=Map("2"->Map("2"->2),"3"->Map("3"->3))
//
//    (a/:b){case(map,(k,v))=>println(map)
//    map}

//    graph1.vertices.take(10).foreach{case(a,b)=>b._1.foreach(v=>print(v._1,':',v._2))}
//    graph1.vertices.take(10).foreach{case(a,b)=>b._1.foreach(v=>print(v._1,':',v._2.values.sum))}
//    graph1.vertices.take(10).foreach{case(a,b)=>println(b._2)}

//    graph1.vertices.foreach()

//    val graph2=graph1.pregel((Map[VertexId,Map[VertexId,Double]]()),20,EdgeDirection.Out)(
//      (id,old,newx)=>(newx),
//      triplet=>{
//        Iterator((triplet.dstId,triplet.srcAttr))
//      }
//      ,(one,two)=>two
//    )

//    graph2.vertices.collect().foreach(println(_))


    //graph.pregel()()

  }
}
