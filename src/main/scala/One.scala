
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object One {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    //println("ssssssssssssssssssssssssss")

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

    val vertexRDD:RDD[(Long,(String,String))]=sc.parallelize(vertexArray)
    val edgeRDD:RDD[Edge[Int]]=sc.parallelize(edgeArray)


    val graph:Graph[(String,String),Int]=Graph(vertexRDD,edgeRDD)
    //graph.vertices.collect.foreach(println)


    graph.vertices.collect().foreach(line=>println(line._2))

    //import scala.collection.mutable.Set

    //graph.mapVertices((vid:VertexId,attr:(String,String))=>(Set[String](),0))

    /***
    val aa=graph.aggregateMessages[Set[String]](triplets=>{
        triplets.sendToDst(Set[String](triplets.srcId.toString))
      },(a,b)=>(a++b))

    val cc=Graph(graph.vertices.leftJoin(aa)((vid,old,vdata)=>vdata),edgeRDD)
      .aggregateMessages[(Set[String],Int)](
      triplets=>{
        val fl=triplets.dstAttr.getOrElse(Set[String](""))
        fl.add(triplets.dstId.toString)
        triplets.sendToSrc((fl,1))
      },(a,b)=>(a._1.&(b._1),a._2+b._2))

    cc.filter{case(vid,(d,c))=>d.size==1}.collect().foreach(println)
      ***/

    //cc.map{case(v,(d,c))=>((d,c),v)}.collect.foreach(println)
    //cc.map{case(v,(d,c))=>((d.size,c),v)}.reduceByKey((a,b)=>if(a>b) a else b).collect.foreach(println)

    //graph.collectNeighbors(EdgeDirection.Either).collect().toSet.foreach(println)

    //graph.inDegrees.collect().toMap.foreach(println)

    //graph.outDegrees.collect().toMap.foreach(println)

    //graph.outDegrees.map{case(vid,a)=>vid}.collect.toSet

    //graph.inDegrees.filter{case(vid:VertexId,attr:Int)=>attr==0}.collect().toMap.foreach(println)
    //graph.outDegrees.filter{case(vid:VertexId,attr:Int)=>attr==0}.collect().toMap.foreach(println)

    //println(graph.vertices.count)

    //cc.collect().foreach(println)
    //cc.map{case(v,(d,c))=>((d,c),v)}.collect().foreach(println)
    //cc.map{case(v,(d,c))=>((d.size,c),v)}.groupByKey().collect.foreach(println)



    //graph.stronglyConnectedComponents()

    //.vertices.collect.foreach(prin
    //graph.vertices.collect.foreach(println)
    //.collect.foreach(println)
    //graph.vertices.collect.foreach(println)
    //.collect.foreach(println)
    //.vertices.collect.take(10).foreach(println)
    /***
    graph.mapEdges(a=>a.attr*2)
    graph.mapTriplets(a=>a.attr*3)
    graph.mapVertices((vid:VertexId,attr:(String,String))=>attr._1.length*attr._2.length)
    graph.aggregateMessages[Int](triplets=>{
      if(triplets.dstAttr._1=="aa")
      triplets.sendToSrc(1)
    },(a,b)=>a+b)

    graph.vertices.filter{case(a,(b,c))=>a==123}
    graph.edges.filter(a=>a.srcId==123)
    ***/
  }
}
