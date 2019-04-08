
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DFAndDS {

  def DFT_Type(sks:SparkSession): Unit ={
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")

    txtFile.columns.foreach(println)   //返回一个string类型的数组，返回值是所有列的名字
    txtFile.dtypes.foreach(println)   //返回一个string类型的二维数组，返回值是所有列的名字以及类型
    txtFile.printSchema()            //打印出字段名称和类型 按照树状结构来打印
    txtFile.schema                   //返回structType 类型，将字段名称和类型按照结构体类型返回

  }

  def DFT_Read(sks:SparkSession): Unit ={

    //读取TxT数据
    //var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")

    //读取CSV数据
    val csvFile=sks.read.csv("file:///D:\\project\\MySparkExample\\data\\cdn.csv")

    // header 是否指定头部行作为schema
    // multiLine 指定这个参数为true，可以将换行的单元格合并为1行。
    // inferSchema 这是自动推断属性列的数据类型。
    // delimiter分隔符，默认为 ,
    // *.csv 支持多个csv文件读取
    val csvFile2=sks.read
      .format("csv")
      .option("header","false")
      .option("multiLine","true")
      .option("inferSchema",true)
      .option("delimiter",",")
      .load("file:///D:\\project\\MySparkExample\\data\\*.csv")

    val csvFile3=sks.read
        .option("header","true")
        .csv("file:///D:\\project\\MySparkExample\\data\\*.csv")

    csvFile.select(col("*")).show(false)
    csvFile2.select(col("*")).show(false)
    csvFile3.select(col("*")).show(false)

  }

  def DFT_Filter(sks:SparkSession): Unit ={
    //读取TxT数据
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")

    //传入筛选条件表达式，可以用and和or
    txtFile
      .where("value like '%a%'").show(false)

    //传入筛选条件表达式,和where使用条件相同
    txtFile
      .filter("value like '%a%'" ).show(false)

    //去除指定字段，保留其他字段 一次只能去除一个字段
    txtFile.drop("value")

  }

  def DFT_Col(sks:SparkSession): Unit ={
    //读取TxT数据
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")

    //选取所有字段展示
    txtFile
      .select(col("*"))
      .show(false)

    //选取字段展示
    txtFile
      .select(col("value"),col("value"))
      .show(false)

    //对字段进行处理
    txtFile
      .select(split(col("value"),"\t").getItem(0).as("id"))
      .show(false)

    //可以对指定字段进行特殊处理
    //可以直接对指定字段调用UDF函数，或者指定别名等

    txtFile
      .selectExpr("value","split(value,'\t') as values")
      .select(
        col("values").getItem(0).as("id"),
        col("values").getItem(1).as("name")
      )
      .show(false)

    txtFile.col("value")   //获取指定字段,返回对象为Column类型

    //根据某个字段内容进行分割，然后生成多行，这时可以使用explode方法
    //txtFile.explode("value" , "values" ){time: String => time.split("\t")}.show(false)
  }

  def DFT_Union(sks:SparkSession): Unit ={
    //读取TxT数据
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")
    txtFile.union(txtFile.limit(1))  //unionAll方法：对两个DataFrame进行组合
  }

  def DFT_With(sks:SparkSession): Unit ={
    //读取TxT数据
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")
    txtFile
      .withColumn("values",split(col("value"), "\t"))
      .withColumn("date1",date_format(current_date(),"yyyyMMdd"))
      .select(
        col("value").as("value"),
        col("values").getItem(0).as("id"),
        col("date1").as("data")
      )
      .show(false)

    //重命名DataFrame中的指定字段名
    txtFile.withColumnRenamed( "id" , "idx" )
  }

  def DFT_Order(sks:SparkSession): Unit ={
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")
    txtFile.orderBy(txtFile("value").desc).show(false)  //按指定字段排序，默认为升序
    txtFile.orderBy(- txtFile("c4")).show(false)    //加个-表示降序排序 sort和orderBy使用方法相同
  }

  def DFT_Collection(sks:SparkSession): Unit ={
    //读取TxT数据
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")

    txtFile.join(txtFile)   //笛卡尔积
    txtFile.join(txtFile, "value")  //类似于a join b using column1的形式，需要两个DataFrame中有相同的一个列名
    txtFile.join(txtFile, Seq("id", "name"))     //还可以using多个字段
    txtFile.join(txtFile, Seq("id", "name"), "inner")  //指定join的类型 ,join操作有inner, outer, left_outer, right_outer, leftsemi类型
    txtFile.join(txtFile , txtFile("value" ) === txtFile( "value"))  //灵活指定join字段的话
    txtFile.join(txtFile , txtFile("value" ) === txtFile( "value"), "inner")

    txtFile.intersect(txtFile.limit(1)).show(false)  //intersect方法可以计算出两个DataFrame中相同的记录
    txtFile.except(txtFile.limit(1)).show(false)     //获取一个DataFrame中有另一个DataFrame中没有的记录
  }

  def DFA_Show(sks:SparkSession): Unit ={
    //读取TxT数据
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")

    //数据展示
    txtFile.first()   //获取第一条数据
    txtFile.head(10)  //获取前10条数据  first和head功能相同
    txtFile.show(100,false)

    txtFile.take(10)   //获取前10条记录。take和takeAsList方法会将获得到的数据返回到Driver端
    txtFile.takeAsList(10) //获取前10条记录并以List形式。take和takeAsList方法会将获得到的数据返回到Driver端，以免Driver发生OutOfMemoryError
    txtFile.limit(3).show()    //limit方法获取指定DataFrame的前n行记录 limit方法不是Action操作
  }

  def DFA_Stat(sks:SparkSession): Unit ={
    //读取TxT数据
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\test.txt")

    //统计类
    //    txtFile.describe("value") //用于统计数值类型字段的统计值，比如count, mean, stddev, min, max等。

    //stat方法可以用于计算指定字段或指定字段之间的统计信息，比如方差，协方差等
    //下面代码演示根据c1字段，统计该字段值出现频率在30%以上的内容。
    // 在jdbcDF中字段c1的内容为"a, b, a, c, d, b"。其中a和b出现的频率为4 / 6，大于0.3
    //jdbcDF.stat.freqItems(Seq ("c1") , 0.3).show()
    txtFile.stat.freqItems(Seq ("value") , 0.4).show(false)

    //    corr(col1,col2,method=None):以双精度值计算DataFrame的量列的相关性。
    //    目前只支持皮尔逊相关系数，DataFrame.corr()是DataFameStatFunctions.corr()的别名。

    //    cov(col1,col2):计算给定列的样本协方差作为双精度值。
    //    DataFrame.cov()是DataFrameStatFunctions.cov() 互为别名。

    //    crosstab(col1,col2):计算给定列的成对频率表. 也被称为应急表.
    //    每列的去重后不同值的数量应小于1e4. 最多1e6非零对频率将被返回.
    //    每行的第一列将是col1的不同值，列名将是col2的不同值.第一列的名称应该为col2.
    //    没有出现的对数将为零. DataFrame.crosstab() and DataFrameStatFunctions.crosstab() 互为别名互为别名。

    //    freqItems(col,support=None):找到列的频繁项，可能有误差。
    //    使用“http://dx.doi.org/10.1145/762471.762473,
    //    proposed by Karp, Schenker, and Papadimitriou”中描述的频繁元素计数算法。
    //    DataFrame.freqItems() and DataFrameStatFunctions.freqItems()互为别名。
    //    注：此功能用于探索性数据分析，因为我们不保证所生成的DataFrame的模式的向后兼容性

    //    sampleBy(col,fraction,seed=None):根据每层上给出的份数返回一个没有更换的分层样本。



  }

  def DFA_Agg(sks:SparkSession): Unit ={
    //读取TxT数据
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")

    txtFile .groupBy("value" )  //根据字段进行group by操作
    txtFile.groupBy( txtFile( "value"))  //可以传入String类型的字段名，也可传入Column类型的对象

    //得到的是GroupedData类型对象，在GroupedData的API中提供了group by之后的操作
    txtFile.groupBy( txtFile( "value")).max("value")   //最大值
    txtFile.groupBy( txtFile( "value")).min("value")   //最小值
    txtFile.groupBy( txtFile( "value")).mean("value")  //平均值
    txtFile.groupBy( txtFile( "value")).sum("value")   //求和
    txtFile.groupBy( txtFile( "value")).count()                    //计数

    //聚合操作调用的是agg方法，该方法有多种调用方式。一般与groupBy方法配合使用
    //对id字段求最大值，对c4字段求和。
    txtFile.agg("value" -> "max", "value" -> "sum")
  }

  def Sql(sks:SparkSession): Unit ={
    var txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\1.txt")
    //SQL语句操作

    txtFile.createTempView("aaaaa")
    val txtFile2=sks.sql("select split(value,'\t')[0] as id,split(value,'\t')[1] as name from aaaaa")
    txtFile2.show(false)

  }


  def main(args:Array[String]): Unit ={

    val sks=SparkSession.builder().master("local").appName("DFAndDS").getOrCreate()

    /**
     * DF Transform操作
     * 只改变DataFrame的形态，而不需要实际取出DataFrame的数据进行计算，都属于转换
     **/

//    DFT_Read(sks)   //读取操作
//    DFT_Col(sks) //列操作

//    DFT_Type(sks)  //类型输出
//    DFT_Filter(sks) //过滤操作
//    DFT_Collection(sks) //集合操作
//    DFT_Union(sks)  //拼接操作
//    DFT_With(sks)   //增加列
//    DFT_Order(sks)

    /**
      * DF Action操作
      * 典型的动作操作有计数(count)，打印表(show)，写(write)等，需要真正地取出数据，会触发Spark的计算
      **/

//    DFA_Show(sks)
    DFA_Stat(sks)

    /**
      * SQL操作
      * 封装SQL命令
      **/

//    Sql(sks)

  }
}
