import breeze.linalg.split
import org.apache.spark.sql.SparkSession

object DFAndDS {
  def main(args:Array[String]): Unit ={

    val sks=SparkSession.builder().master("local").appName("DFAndDS").getOrCreate()
    //val txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\graphx-wiki-vertices.txt")
    val txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")

    //txtFile.describe("value") //用于统计数值类型字段的统计值，比如count, mean, stddev, min, max等。

//    txtFile.first()   //获取第一条数据
//    txtFile.head(10)  //获取前10条数据  first和head功能相同
//
//    txtFile.show(100,false)
//
//    txtFile.take(10)   //获取前10条记录。take和takeAsList方法会将获得到的数据返回到Driver端
//    txtFile.takeAsList(10) //获取前10条记录并以List形式。take和takeAsList方法会将获得到的数据返回到Driver端，以免Driver发生OutOfMemoryError
//
//    txtFile.where("value like '%a%'").show()   //传入筛选条件表达式，可以用and和or
//    txtFile.filter("value like '%a%'" ).show()  //传入筛选条件表达式,和where使用条件相同

//    txtFile.select("value").show() //获取指定字段值
//
//    txtFile.select(txtFile( "value" ), txtFile( "value").as("bbb") ).show( false) //而是传入Column类型参数
//      txtFile.selectExpr(split("value","\t"))
//      txtFile.withColumn("value",split(txtFile("value"), ",")).show()

      txtFile.createTempView("aaaaa")
      val txtFile2=sks.sql("select split(value,'\t')[0] as id,split(value,'\t')[1] as name from aaaaa")
      txtFile2.show(false)


    //      txtFile.selectExpr("value","value.split('\t')").show   //可以对指定字段进行特殊处理
//    txtFile.col("value")   //获取指定字段,返回对象为Column类型
//    txtFile.limit(3).show()    //limit方法获取指定DataFrame的前n行记录 limit方法不是Action操作
//
//    txtFile.orderBy(txtFile("value").desc).show(false)  //按指定字段排序，默认为升序
//    txtFile.orderBy(- txtFile("c4")).show(false)    //加个-表示降序排序 sort和orderBy使用方法相同
//
//    txtFile .groupBy("value" )  //根据字段进行group by操作
//    txtFile.groupBy( txtFile( "value"))  //可以传入String类型的字段名，也可传入Column类型的对象
//
//    //得到的是GroupedData类型对象，在GroupedData的API中提供了group by之后的操作
//    txtFile.groupBy( txtFile( "value")).max("value")   //最大值
//    txtFile.groupBy( txtFile( "value")).min("value")   //最小值
//    txtFile.groupBy( txtFile( "value")).mean("value")  //平均值
//    txtFile.groupBy( txtFile( "value")).sum("value")   //求和
//    txtFile.groupBy( txtFile( "value")).count()                    //计数

    //聚合操作调用的是agg方法，该方法有多种调用方式。一般与groupBy方法配合使用
    //对id字段求最大值，对c4字段求和。
//    txtFile.agg("value" -> "max", "value" -> "sum")
//
//
//    txtFile.union(txtFile.limit(1))  //unionAll方法：对两个DataFrame进行组合
//
//
//    txtFile.drop("value")  //去除指定字段，保留其他字段 一次只能去除一个字段

//    txtFile.columns.foreach(println)   //返回一个string类型的数组，返回值是所有列的名字
//    txtFile.dtypes.foreach(println)   //返回一个string类型的二维数组，返回值是所有列的名字以及类型
//    txtFile.printSchema()            //打印出字段名称和类型 按照树状结构来打印
//    txtFile.schema                   //返回structType 类型，将字段名称和类型按照结构体类型返回

//    txtFile.withColumnRenamed( "id" , "idx" )  //重命名DataFrame中的指定字段名
//    txtFile.withColumn("id2", txtFile("id")).show( false)  //往当前DataFrame中新增一列

    //根据某个字段内容进行分割，然后生成多行，这时可以使用explode方法
//    txtFile.explode("value" , "values" ){time: String => time.split("\t")}.show(false)


//    txtFile.join(txtFile)   //笛卡尔积
//    txtFile.join(txtFile, "value")  //类似于a join b using column1的形式，需要两个DataFrame中有相同的一个列名
//    txtFile.join(txtFile, Seq("id", "name"))     //还可以using多个字段
//    txtFile.join(txtFile, Seq("id", "name"), "inner")  //指定join的类型 ,join操作有inner, outer, left_outer, right_outer, leftsemi类型
//    txtFile.join(txtFile , txtFile("value" ) === txtFile( "value"))  //灵活指定join字段的话
//    txtFile.join(txtFile , txtFile("value" ) === txtFile( "value"), "inner")
//
//
//    txtFile.intersect(txtFile.limit(1)).show(false)  //intersect方法可以计算出两个DataFrame中相同的记录
//    txtFile.except(txtFile.limit(1)).show(false)     //获取一个DataFrame中有另一个DataFrame中没有的记录


    //stat方法可以用于计算指定字段或指定字段之间的统计信息，比如方差，协方差等
    //下面代码演示根据c1字段，统计该字段值出现频率在30%以上的内容。
    // 在jdbcDF中字段c1的内容为"a, b, a, c, d, b"。其中a和b出现的频率为4 / 6，大于0.3
    //jdbcDF.stat.freqItems(Seq ("c1") , 0.3).show()

//    txtFile.stat.freqItems(Seq ("value") , 0.3).show()


  }
}
