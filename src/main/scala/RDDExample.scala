import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

object RDDExample {

  def RDDT_Map(sc:SparkContext): Unit ={

    //sc.parallelize(1 to 100,5)表示将1 to 100产生的集合（Range）转换成一个RDD
    // //并创建5个partition
    val rdd1=sc.parallelize(1 to 100,5)

    //对集合中每个元素进行操作。
    //数据集中的每个元素经过用户自定义的函数转换形成一个新的RDD，新的RDD叫MappedRDD
    rdd1.map(line=>(line+100,line)).map(line=>line._1+line._2).collect().foreach(println)
    rdd1.map(line=>(line+100,line)).map{case(a,b)=>a+b}.collect().foreach(println)

//    rdd
    sc.stop()
  }

  def RDDT_FlatMap(sc:SparkContext): Unit ={

    val rdd = sc.parallelize(1 to 5)

    //对集合中每个元素进行操作然后再扁平化。
    //所以flatMap扁平话意思大概就是先用了一次map之后对全部数据再一次map。
    //与map类似，但每个元素输入项都可以被映射到0个或多个的输出项，最终将结果”扁平化“后输出

    val fm = rdd.flatMap(x => (1 to x)).collect()
    fm.foreach( x => println(x + " "))

    println("-----------")
    rdd.map(x=>(1 to x)).collect().foreach(println)
    println("-----------")

    val arr=sc.parallelize(Array(("A",1),("B",2),("C",3)))
    arr.flatMap(x=>(x._1+x._2)).foreach(println)

    val arr2=sc.parallelize(List("A;B;C;D;B;D;C","B;D;A;E;D;C","A;B"))
    val arr3=arr2.map(_.split(";"))
    arr3.collect().foreach(x=>println(x.toList))

    arr3.map(x=>{
      for(i<-0 until x.length-1) yield (x(i)+","+x(i+1),1)
    }).collect().foreach(println)

    arr3.flatMap(x=>{
      for(i<-0 until x.length-1) yield (x(i)+","+x(i+1),1)
    }).collect().foreach(println)
  }

  def RDDT_MapPartitions(sc:SparkContext): Unit ={
//    arrayRDD.mapPartitions(datas=>{
    ////      dbConnect = getDbConnect() //获取数据库连接
    ////      datas.foreach(data=>{
    ////        dbConnect.insert(data) //循环插入数据
    ////      })
    ////      dbConnect.commit() //提交数据库事务
    ////      dbConnect.close() //关闭数据库连接
    ////    })


//    /mapPartitions每次处理一批数据
      //将 arrayRDD分成x批数据进行处理
      //elements是其中一批数据
      //mapPartitions返回一批数据（iterator）
      val arrayRDD =sc.parallelize(Array(1,2,3,4,5,6,7,8,9))

      arrayRDD.mapPartitions(elements=>{
        var result = new ArrayBuffer[Int]()
        elements.foreach(element=>{
          result.+=(element)
        })
        result.iterator
      }).foreach(println)

  }

  def RDDT_Sample(sc:SparkContext): Unit ={
    //sample(withReplacement,fraction,seed)
    // 指定的随机种子随机抽样出数量为fraction的数据
    //withReplacement表示是抽出的数据是否放回
    //true为有放回的抽样，false为无放回的抽样
    //从RDD中随机且有放回的抽出50%的数据，随机种子值为3（即可能以1 2 3的其中一个起始值）

    val rdd = sc.parallelize(1 to 10)
    val sample1 = rdd.sample(true,0.3,0)
    sample1.collect.foreach(x => print(x + " "))
    sc.stop


  }

  def RDDT_Union(sc:SparkContext): Unit ={
    //union(ortherDataset)
    // 将两个RDD中的数据集进行合并，最终返回两个RDD的并集，
    // RDD中存在相同的元素也不会去重

    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(3 to 5)
    val unionRDD = rdd1.union(rdd2)
    unionRDD.collect.foreach(x => print(x + " "))
    sc.stop()
  }

  def RDDT_Intersection(sc:SparkContext): Unit ={

    //intersection(otherDataset):返回两个RDD的交集

    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(3 to 5)
    val unionRDD = rdd1.intersection(rdd2)
    unionRDD.collect.foreach(x => print(x + " "))
    sc.stop()

  }

  def RDDT_Distinct(sc:SparkContext): Unit ={
    //distinct([numTasks]):
    // 对RDD中的元素进行去重

    val list = List(1,1,2,5,2,9,6,1)
    val distinctRDD = sc.parallelize(list)
    val unionRDD = distinctRDD.distinct()
    unionRDD.collect.foreach(x => print(x + " "))

  }

  def RDDT_Cartesian(sc:SparkContext): Unit ={
    //cartesian(otherDataset)
    // 对两个RDD中的所有元素进行笛卡尔积操作

    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(2 to 5)
    val cartesianRDD = rdd1.cartesian(rdd2)
    cartesianRDD.foreach(x => println(x + " "))

  }

  def RDDT_Coalesce(sc:SparkContext): Unit ={
    //coalesce(numPartitions，shuffle):
    // 对RDD的分区进行重新分区，shuffle默认值为false,当shuffle=false时，
    // 不能增加分区数目,但不会报错，只是分区个数还是原来的

    val rdd = sc.parallelize(1 to 16,4)
    val coalesceRDD = rdd.coalesce(3) //当suffle的值为false时，不能增加分区数(即分区数不能从5->7)
    println("重新分区后的分区个数:"+coalesceRDD.partitions.size)

    val rdd2 = sc.parallelize(1 to 16,4)
    val coalesceRDD2 = rdd2.coalesce(7,true)
    println("重新分区后的分区个数:"+coalesceRDD2.partitions.size)
    println("RDD依赖关系:"+coalesceRDD2.toDebugString)

  }

  def RDDT_Repartition(sc:SparkContext): Unit ={
    //repartition(numPartition):是函数coalesce(numPartition,true)的实现，
    // 效果和例9.1的coalesce(numPartition,true)的一样
  }

  def RDDT_Glom(sc:SparkContext): Unit ={
    //glom():将RDD的每个分区中的类型为T的元素转换换数组Array[T]

    val rdd = sc.parallelize(1 to 16,4)
    val glomRDD = rdd.glom() //RDD[Array[T]]
    glomRDD.foreach(rdd => println(rdd.getClass.getSimpleName))
    sc.stop()

  }

  def RDDT_RandomSplit(sc:SparkContext): Unit ={
    //randomSplit(weight:Array[Double],seed):
    // 根据weight权重值将一个RDD划分成多个RDD,权重越高划分得到的元素较多的几率就越大
    val rdd = sc.parallelize(1 to 10)
    val randomSplitRDD = rdd.randomSplit(Array(1.0,2.0,7.0))
    randomSplitRDD(0).foreach(x => print(x +" "))
    randomSplitRDD(1).foreach(x => print(x +" "))
    randomSplitRDD(2).foreach(x => print(x +" "))
    sc.stop
  }

  def RDDT_mapValus(sc:SparkContext): Unit ={
    //mapValus(fun):对[K,V]型数据中的V值map操作

    val list = List(("mobin",22),("kpop",20),("lufei",23))
    val rdd = sc.parallelize(list)
    val mapValuesRDD = rdd.mapValues(_+2)
    mapValuesRDD.foreach(println)

  }

  def RDDT_flatMapValues(sc:SparkContext): Unit ={
    //flatMapValues(fun)：对[K,V]型数据中的V值flatmap操作
    val list = List(("mobin",22),("kpop",20),("lufei",23))
    val rdd = sc.parallelize(list)
    val mapValuesRDD = rdd.flatMapValues(x => Seq(x,"male"))
    mapValuesRDD.foreach(println)
  }

  def RDDT_comineByKey(sc:SparkContext): Unit ={
    //comineByKey(createCombiner,mergeValue,mergeCombiners,partitioner,mapSideCombine)
    //   comineByKey(createCombiner,mergeValue,mergeCombiners,numPartitions)
    //   comineByKey(createCombiner,mergeValue,mergeCombiners)

    //createCombiner:在第一次遇到Key时创建组合器函数，将RDD数据集中的V类型值转换C类型值（V => C），
    //mergeValue：合并值函数，再次遇到相同的Key时，将createCombiner道理的C类型值与这次传入的V类型值合并成一个C类型值（C,V）=>C，
    //mergeCombiners:合并组合器函数，将C类型值两两合并成一个C类型值
    //partitioner：使用已有的或自定义的分区函数，默认是HashPartitioner
    //mapSideCombine：是否在map端进行Combine操作,默认为true


    //注意前三个函数的参数类型要对应；第一次遇到Key时调用createCombiner，
    // 再次遇到相同的Key时调用mergeValue合并值

    val people = List(("male", "Mobin"), ("male", "Kpop"), ("female", "Lucy"), ("male", "Lufei"), ("female", "Amy"))
    val rdd = sc.parallelize(people)
    val combinByKeyRDD = rdd.combineByKey(
      (x: String) => (List(x), 1),
      (peo: (List[String], Int), x : String) => (x :: peo._1, peo._2 + 1),
      (sex1: (List[String], Int), sex2: (List[String], Int)) => (sex1._1 ::: sex2._1, sex1._2 + sex2._2))
    combinByKeyRDD.foreach(println)
    sc.stop()


  }

  def RDDT_foldByKey(sc:SparkContext): Unit ={
    //foldByKey(zeroValue)(func)
    //foldByKey(zeroValue,partitioner)(func)
    //foldByKey(zeroValue,numPartitiones)(func)
   // foldByKey函数是通过调用CombineByKey函数实现的
    //zeroVale：对V进行初始化，实际上是通过CombineByKey的createCombiner实现的  V =>  (zeroValue,V)，再通过func函数映射成新的值，即func(zeroValue,V),如例4可看作对每个V先进行  V=> 2 + V
    //func: Value将通过func函数按Key值进行合并（实际上是通过CombineByKey的mergeValue，mergeCombiners函数实现的，只不过在这里，这两个函数是相同的）

    val people = List(("Mobin", 2), ("Mobin", 1), ("Lucy", 2), ("Amy", 1), ("Lucy", 3))
    val rdd = sc.parallelize(people)
    val foldByKeyRDD = rdd.foldByKey(2)(_+_)
    foldByKeyRDD.foreach(println)
  }

  def RDDT_reduceByKey(sc:SparkContext): Unit ={
    //reduceByKey(func,numPartitions)
    // 按Key进行分组，使用给定的func函数聚合value值, numPartitions设置分区数，提高作业并行度

    val arr = List(("A",3),("A",2),("B",1),("B",3))
    val rdd = sc.parallelize(arr)
    val reduceByKeyRDD = rdd.reduceByKey(_ +_)
    reduceByKeyRDD.foreach(println)
    sc.stop()

  }

  def RDDT_groupByKey(sc:SparkContext): Unit ={
    //groupByKey(numPartitions)
    // 按Key进行分组，返回[K,Iterable[V]]，numPartitions设置分区数，提高作业并行度
    val arr = List(("A",1),("B",2),("A",2),("B",3))
    val rdd = sc.parallelize(arr)
    val groupByKeyRDD = rdd.groupByKey()
    groupByKeyRDD.foreach(println)
    sc.stop
  }

  def RDDT_sortByKey(sc:SparkContext): Unit ={
    //sortByKey(accending，numPartitions)
    // 返回以Key排序的（K,V）键值对组成的RDD，accending为true时表示升序，为false时表示降序，
    // numPartitions设置分区数，提高作业并行度

    val arr = List(("A",1),("B",2),("A",2),("B",3))
    val rdd = sc.parallelize(arr)
    val sortByKeyRDD = rdd.sortByKey()
    sortByKeyRDD.foreach(println)
    sc.stop

  }

  def RDDT_cogroup(sc:SparkContext): Unit ={
    //cogroup(otherDataSet，numPartitions)
    // 对两个RDD(如:(K,V)和(K,W))相同Key的元素先分别做聚合，最后返回(K,Iterator<V>,Iterator<W>)形式的RDD,
    // numPartitions设置分区数，提高作业并行度

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd1 = sc.parallelize(arr, 3)
    val rdd2 = sc.parallelize(arr1, 3)
    val groupByKeyRDD = rdd1.cogroup(rdd2)
    groupByKeyRDD.foreach(println)
    sc.stop

  }

  def RDDT_join(sc:SparkContext): Unit ={
    //    join(otherDataSet,numPartitions)
    //    对两个RDD先进行cogroup操作形成新的RDD，再对每个Key下的元素进行笛卡尔积，
    //    numPartitions设置分区数，提高作业并行度

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    val groupByKeyRDD = rdd.join(rdd1)
    groupByKeyRDD.foreach(println)


  }

  def RDDT_LeftOutJoin(sc:SparkContext): Unit ={
    //LeftOutJoin(otherDataSet，numPartitions)
    // 左外连接，包含左RDD的所有数据，如果右边没有与之匹配的用None表示,
    // numPartitions设置分区数，提高作业并行度

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3),("C",1))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    val leftOutJoinRDD = rdd.leftOuterJoin(rdd1)
    leftOutJoinRDD .foreach(println)
    sc.stop

  }

  def RDDT_RightOutJoin(sc:SparkContext): Unit ={
    //    RightOutJoin(otherDataSet, numPartitions)
    //    右外连接，包含右RDD的所有数据，如果左边没有与之匹配的用None表示,
    //    numPartitions设置分区数，提高作业并行度

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val arr1 = List(("A", "A1"), ("B", "B1"), ("A", "A2"), ("B", "B2"),("C","C1"))
    val rdd = sc.parallelize(arr, 3)
    val rdd1 = sc.parallelize(arr1, 3)
    val rightOutJoinRDD = rdd.rightOuterJoin(rdd1)
    rightOutJoinRDD.foreach(println)
    sc.stop

  }



  def RDDA_reduce(sc:SparkContext): Unit ={
    //    reduce(func)
    //    通过函数func先聚集各分区的数据集，再聚集分区之间的数据，
    //    func接收两个参数，返回一个新值，新值再做为参数继续传递给函数func，直到最后一个元素
  }

  def RDDA_collect(sc:SparkContext): Unit ={
    //    collect()
    //    以数据的形式返回数据集中的所有元素给Driver程序，为防止Driver程序内存溢出，
    //    一般要控制返回的数据集大小
  }

  def RDDA_count(sc:SparkContext): Unit ={
    //    count()：返回数据集元素个数
  }

  def RDDA_first(sc:SparkContext): Unit ={
    //    first():返回数据集的第一个元素
  }

  def RDDA_take(sc:SparkContext): Unit ={
    //    take(n):以数组的形式返回数据集上的前n个元素
  }

  def RDDA_top(sc:SparkContext): Unit ={
      //    top(n):按默认或者指定的排序规则返回前n个元素，默认按降序输出
  }

  def RDDA_takeOrdered(sc:SparkContext): Unit ={
    //    takeOrdered(n,[ordering]): 按自然顺序或者指定的排序规则返回前n个元素

    val rdd = sc.parallelize(1 to 10,2)
    val reduceRDD = rdd.reduce(_ + _)
    val reduceRDD1 = rdd.reduce(_ - _) //如果分区数据为1结果为 -53
    val countRDD = rdd.count()
    val firstRDD = rdd.first()
    val takeRDD = rdd.take(5)    //输出前个元素
    val topRDD = rdd.top(3)      //从高到底输出前三个元素
    val takeOrderedRDD = rdd.takeOrdered(3)    //按自然顺序从底到高输出前三个元素

    println("func +: "+reduceRDD)
    println("func -: "+reduceRDD1)
    println("count: "+countRDD)
    println("first: "+firstRDD)
    println("take:")
    takeRDD.foreach(x => print(x +" "))
    println("\ntop:")
    topRDD.foreach(x => print(x +" "))
    println("\ntakeOrdered:")
    takeOrderedRDD.foreach(x => print(x +" "))
    sc.stop
  }

  def RDDA_countByKey(sc:SparkContext): Unit ={
      //    countByKey():作用于K-V类型的RDD上，统计每个key的个数，返回(K,K的个数)
  }

  def RDDA_collectAsMap(sc:SparkContext): Unit ={
      //    collectAsMap()
    //    作用于K-V类型的RDD上，作用与collect不同的是collectAsMap函数不包含重复的key，
    //    对于重复的key。后面的元素覆盖前面的元素


  }

  def RDDA_lookup(sc:SparkContext): Unit ={
    //    lookup(k)：作用于K-V类型的RDD上，返回指定K的所有V值

    val arr = List(("A", 1), ("B", 2), ("A", 2), ("B", 3))
    val rdd = sc.parallelize(arr,2)
    val countByKeyRDD = rdd.countByKey()
    val collectAsMapRDD = rdd.collectAsMap()

    println("countByKey:")
    countByKeyRDD.foreach(print)

    println("\ncollectAsMap:")
    collectAsMapRDD.foreach(print)
    sc.stop

  }

  def RDDA_aggregate(sc:SparkContext): Unit ={
    //    aggregate(zeroValue:U)(seqOp:(U,T) => U,comOp(U,U) => U):
    //    seqOp函数将每个分区的数据聚合成类型为U的值，comOp函数将各分区的U类型数据聚合起来得到类型为U的值

    val rdd = sc.parallelize(List(1,2,3,4),2)
    val aggregateRDD = rdd.aggregate(2)(_+_,_ * _)
    println(aggregateRDD)
    sc.stop

  }

  def RDDA_fold(sc:SparkContext): Unit ={
      //    fold(zeroValue:T)(op:(T,T) => T)
    //    通过op函数聚合各分区中的元素及合并各分区的元素，op函数需要两个参数，在开始时第一个传入的参数为zeroValue,T为RDD数据集的数据类型
    //    其作用相当于SeqOp和comOp函数都相同的aggregate函数

    val rdd = sc.parallelize(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)), 2)
    val foldRDD = rdd.fold(("d", 0))((val1, val2) => { if (val1._2 >= val2._2) val1 else val2})
    println(foldRDD)

  }

  def RDDA_saveAsFile(sc:SparkContext): Unit ={
    //    saveAsFile(path:String):将最终的结果数据保存到指定的HDFS目录中
  }

  def RDDA_saveAsSequenceFile(sc:SparkContext): Unit ={
    //    saveAsSequenceFile(path:String):将最终的结果数据以sequence的格式保存到指定的HDFS目录中
  }

  def RDDT_CheckPoint(sc:SparkContext): Unit ={
//    rdd.toDebugString  查看rdd debug信息
//    sc.setCheckpointDir("hdfs://lijie:9000/checkpoint0727")
//    val rdd = sc.parallelize(1 to 10000)
//    rdd.cache()
//    rdd.checkpoint()
//    rdd.collect

  }

  def RDD_Basic(sc:SparkContext): Unit ={

    val rdd = sc.parallelize(1 to 10000)
    rdd.partitions.size  //查看分区个数


    rdd.dependencies.foreach(dep=>{
      println("dependency type:"+dep.getClass)
      println("dependency RDD:"+dep.rdd)
      println("dependency partitions:"+dep.rdd.partitions)
      println("dependency partitions size:"+dep.rdd.partitions.length)
    })

  }

  def main(args:Array[String]): Unit ={
    val sc=SparkSession.builder()
      .master("local")
      .appName("RDDTest")
      .getOrCreate()
      .sparkContext

    /**
    *
    * RDD基本属性
    */

//    RDD_Basic(sc)

      /**
      * RDD基本转换
      * */

//    RDDT_Map(sc)
//    RDDT_FlatMap(sc)
//    RDDT_Sample(sc)
//      RDDT_Union(sc)
//    RDDT_Intersection(sc)
//    RDDT_Distinct(sc)
//    RDDT_Cartesian(sc)
//    RDDT_Coalesce(sc)
//    RDDT_Glom(sc)
//    RDDT_RandomSplit(sc)
//    RDDT_CheckPoint(sc)

    /**
      * 键-值RDD转换
      * */

//    RDDT_mapValus(sc)
//    RDDT_flatMapValues(sc)
//    RDDT_comineByKey(sc)
//    RDDT_foldByKey(sc)
//    RDDT_reduceByKey(sc)
//    RDDT_sortByKey(sc)
//    RDDT_cogroup(sc)
//    RDDT_join(sc)
//    RDDT_LeftOutJoin(sc)
//    RDDT_RightOutJoin(sc)


    /**
      * Action操作
      * */

//    RDDA_reduce(sc)
//    RDDA_collect(sc)
//    RDDA_count(sc)
//    RDDA_first(sc)
//    RDDA_take(sc)
//    RDDA_top(sc)
//    RDDA_takeOrdered(sc)
//    RDDA_countByKey(sc)
//    RDDA_collectAsMap(sc)
//    RDDA_lookup(sc)
//    RDDA_aggregate(sc)
//    RDDA_fold(sc)
//    RDDA_saveAsFile(sc)
//    RDDA_saveAsSequenceFile(sc)

  }
}
