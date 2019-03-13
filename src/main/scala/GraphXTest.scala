import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

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

    println("转换操作")
    println("**********************************************************")
    println("顶点的转换操作，顶点age + 10：")
    graph.mapVertices{ case (id, (name, age)) => (id, (name, age+10))}
      .vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println
    println("边的转换操作，边的属性*2：")
    graph.mapEdges(e=>e.attr*2).edges.collect
      .foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    //graph.mapVertices((a,b)=>(b._2,b._1)).vertices.collect().foreach(println)
    graph.mapEdges(edges=>edges.attr).edges.collect().foreach(println)

    // 使用mapTriplets对三元组整体进行操作
    graph.mapTriplets(triplets=>triplets.attr).edges.collect().foreach(println)

      //aggregateMessage函数有两个大操作，
      // 一个是sendMsg，
      // 一个是mergeMsg。aggregateMessages函数其对象是三元组
      //    sendMsg是将三元组的属性信息进行转发，mergeMsg是将sendMsg转发的内容进行聚合。
      //    sendMsg函数以EdgeContex作为输入参数，没返回值，提供两个消息的函数
      //    sendToSrc：将Msg类型的消息发送给源节点
      //    sendToDst:将Msg类型消息发送给目的节点。

    graph.aggregateMessages[(String,String)](triplets=>{
        triplets.sendToDst((triplets.srcAttr._1.toString,triplets.srcAttr._2.toString))
    },(a,b)=>a)

    //1.求图的入度
    graph.aggregateMessages[Int](_.sendToDst(1),_+_).collect().foreach(println)

    //2.求图的出度
    graph.aggregateMessages[Int](_.sendToSrc(1),_+_).collect().foreach(println)

    //    每个图是由3个RDD组成，所以会占用更多的内存。相应图的cache、unpersist和checkpoint，更需要注意使用技巧。
    //    出于最大限度复用边的理念，GraphX的默认接口只提供了unpersistVertices方法。
    //    如果要释放边，调用g.edges.unpersist()方法才行，这给用户带来了一定的不便，但为GraphX的优化提供了便利和空间。
    //    参考GraphX的Pregel代码，对一个大图，目前最佳的实践是：


//    大体之意是根据GraphX中Graph的不变性，对g做操作并赋回给g之后，g已不是原来的g了，而且会在下一轮迭代使用，
//    所以必须cache。另外，必须先用prevG保留住对原来图的引用，并在新图产生后，快速将旧图彻底释放掉。
//    否则，十几轮迭代后，会有内存泄漏问题，很快耗光作业缓存空间。
//    var g_bak:Graph[(String,String),Int]=null
//    while(true){
//      g_bak=graph
//      //graph=graph.dosomethings("")
//      graph.cache()
//      g_bak.unpersistVertices()
//      g_bak.edges.unpersist()
//    }

//    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
//      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
//    }
//
//    //Degrees操作
//    println("找出图中最大的出度、入度、度数：")
//    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
//      if (a._2 > b._2) a else b
//    }
//    println("max of outDegrees:" +
//      graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" +
//      graph.degrees.reduce(max))


//    println("**********************************************************")
//    println("结构操作")
//    println("**********************************************************")
//    println("顶点年纪>30的子图：")
//    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
//    println("子图所有顶点：")
//    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
//    println
//    println("子图所有边：")
//    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
//    println


//    println("**********************************************************")
//    println("连接操作")
//    println("**********************************************************")
//    val inDegrees: VertexRDD[Int] = graph.inDegrees
//    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
//
//    //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
//    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0)}
//
//    //initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值
//    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
//      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
//    }.outerJoinVertices(initialUserGraph.outDegrees) {
//      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg,outDegOpt.getOrElse(0))
//    }
//    println("连接图的属性：")
//    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
//    println
//    println("出度和入读相同的人员：")
//    userGraph.vertices.filter {
//      case (id, u) => u.inDeg == u.outDeg
//    }.collect.foreach {
//      case (id, property) => println(property.name)
//    }
//    println


//***********************************************************************************
//***************************  实用操作    ****************************************
//**********************************************************************************
//    println("**********************************************************")
//    println("聚合操作")
//    println("**********************************************************")
//    println("找出5到各顶点的最短：")
//    val sourceId: VertexId = 5L // 定义源点
//    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
//    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
//      (id, dist, newDist) => math.min(dist, newDist),
//      triplet => {  // 计算权重
//        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
//          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
//        } else {
//          Iterator.empty
//        }
//      },
//      (a,b) => math.min(a,b) // 最短距离
//    )
//    println(sssp.vertices.collect.mkString("\n"))
  }

  def vertciesTools(vertices:VertexRDD[(String,String)]): Unit ={
    vertices.collect.foreach(println(_))

    // 使用filter筛选出属性value>150的顶点
    val filteredVertices:VertexRDD[(String, String)] =
      vertices.filter{ case (vid:VertexId, (name:String, value:String)) => name=="c" }
    filteredVertices.collect.foreach(println(_))

    vertices.filter { case (id, (name, age)) => age=="aa"}.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }


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
//    edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

  }

  def main(args:Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sks=SparkSession.builder()
      .master("local")
      .appName("graphxTest")
      .getOrCreate()

    val graph=getGrahp(sks)
    graphxBasic(graph)
//    graph.collectNeighbors(EdgeDirection.Out).collect().foreach(println)

    //    属性的操作
    //    结构的操作
    //    链接操作

    val vertices=graph.vertices
//    vertciesTools(vertices)

    val edges=graph.edges
//    edgesTools(edges)
    //graph.vertices.collect().foreach(println)

//    //基本信息接口
//    graph.numEdges
//    graph.numVertices
//    graph.edges(in/out)

//    转换操作
//    graph.mapVertices()
//    graph.mapEdges()
//    graph.mapTriplets()

//    结构操作
//    graph.reverse
//    graph.subgraph()
//    graph.mask()
//    graph.groupEdges()

//    关联操作
//    graph.joinVertices()
//    graph.outerJoinVertices()

//    聚合操作
//    graph.aggregateMessages()
//    graph.collectNeighbors()

//    缓存操作
//    graph.persist()
//    graph.cache
//    graph.unpersist()
  }
}
