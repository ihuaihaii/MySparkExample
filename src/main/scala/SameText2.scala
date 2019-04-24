import java.util.Arrays
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer}
import org.apache.spark.sql.functions._
import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{SparseVector, Vectors}

object SameText2 {

  var spark:SparkSession = _

  def main(args:Array[String]): Unit ={
    val spark=initSpark();

    //阶段一  完成 TF-IDF
    val rawTranningData=getTrainData(spark)
    val trainningTfidfData=tfidf(rawTranningData)
    val trainFinalData=spark.sparkContext.broadcast(trainningTfidfData)

    //阶段二
    val rawNewData=getNewData(spark)
    val newTfidfData=tfidf(rawNewData)
    val newFinalData=spark.sparkContext.broadcast(newTfidfData)
    val rs=pickupTheTopSimilarShop(newFinalData,trainFinalData)

    val rs1=rs.select(
      col("id"),
      col("text") as "source",
      //col("forecastValue") as "forecastValue",
      split(col("forecastValue"),"\\|").getItem(0).as("target"),
      split(col("forecastValue"),"\\|").getItem(1).as("Similarity"))
      .withColumn("target",regexp_replace(col("target"),"[\\[\\]]",""))
      .withColumn("Similarity",regexp_replace(col("Similarity"),"[\\[\\]]",""))
    rs1.printSchema()
    rs1.show(false)

  }


  def pickupTheTopSimilarShop(newTfidfData:Broadcast[DataFrame],trainningTfidfData:Broadcast[DataFrame]): DataFrame ={
    val BLAS_Dot=udf{(x:SparseVector,y:SparseVector)=>
      val xValues = x.values
      val xIndices = x.indices
      val yValues = y.values
      val yIndices = y.indices
      val nnzx = xIndices.length
      val nnzy = yIndices.length

      var kx = 0
      var ky = 0
      var sum = 0.0
      // y catching x
      while (kx < nnzx && ky < nnzy) {
        val ix = xIndices(kx)
        while (ky < nnzy && yIndices(ky) < ix) {
          ky += 1
        }
        if (ky < nnzy && yIndices(ky) == ix) {
          sum += xValues(kx) * yValues(ky)
          ky += 1
        }
        kx += 1
      }
      sum
    }
    val Vector_Norm=udf{(data:SparseVector)=>Vectors.norm((data.toSparse), 2.0)}
    val similarty=udf{(dot:Double,v1:Double,v2:Double)=>dot/(v1*v2)}
    val getSize=udf{(newRowData:SparseVector)=>newRowData.size}

    val forecastValue = udf{(newRowData: SparseVector) =>{
      val addVector=udf{(flag:String)=>newRowData}
      val trainningData=trainningTfidfData.value
                            .select(col("id"),col("text"),col("flag"),col("features"))
                            .withColumn("newF",addVector(col("flag")))
                            .withColumn("BLAS_Dot",BLAS_Dot(col("newF"),col("features")))
                            .withColumn("v1",Vector_Norm(col("newF")))
                            .withColumn("v2",Vector_Norm(col("features")))
                            .withColumn("rs",col("BLAS_Dot")/(col("v1")*col("v2")))

      val rsdata=trainningData.orderBy(col("rs").desc).limit(1)
      val value=rsdata.select("rs").collect().toList(0)
      val cata=rsdata.select("text").collect().toList(0)

     cata+"|"+value
    }}

    return newTfidfData.value
      .select(col("id"),col("text"),col("features"))
      .withColumn("forecastValue",forecastValue(col("features")))
  }

  def getTrainningFeatureRDD(trainningTfidfDataset: DataFrame): DataFrame ={
    val vector_size = udf{(features: SparseVector) => features.size.toString}
    val vector_indices = udf{(features: SparseVector) => Arrays.toString(features.indices)}
    val vector_values = udf{(features: SparseVector) => Arrays.toString(features.values)}

    val rs=trainningTfidfDataset.select(col("id"),col("flag"),col("features"))
        .withColumn("vector_size",vector_size(col("features")))
        .withColumn("vector_indices",vector_indices(col("features")))
        .withColumn("vector_values",vector_values(col("features")))

    return rs.select(col("id"),col("flag"),col("features"),col("vector_size"),col("vector_indices"),col("vector_values"))
  }

  def initSpark(): SparkSession ={
    if(spark==null){
      return SparkSession.builder().appName("same text minging").master("local").getOrCreate()
    }else{
      return spark
    }
  }

  def getTrainData(spark:SparkSession): DataFrame ={
    return  spark.sqlContext.createDataFrame(Seq(
//      (0, "总行发展规划部机构发展部"),
//      (1, "总行发展规划部投资管理部"),
//      (2, "总行发展规划部策略研究部"),
//      (3, "南京银行东台支行"),
//      (4, "南京银行滨海支行"),
//      (5, "南京银行丹阳支行"),
//      (6, "南京银行上海分行专营"),
//      (7, "南京银行无锡分行专营"),
//      (8, "南京银行南通分行专营")
      (0, "南京市龙池地铁站"),
      (1, "南京长江路"),
      (2, "南京市新街口"),
      (3, "南京市紫峰大厦"),
      (4, "长江大桥"),
      (5, "南京地区"),
      (6, "南京鼓楼区大润发"),
      (7, "上海市南京路99号")
    )).toDF("id", "flag")
  }

  def getNewData(spark:SparkSession): DataFrame ={
    return  spark.sqlContext.createDataFrame(Seq(
//      (0, "总行发展规划部"),
//      (1, "发展规划部"),
//      (2, "总行信息技术部"),
//      (3, "盐城分行东台支行"),
//      (4, "盐城分行滨海支行"),
//      (5, "镇江分行丹阳支行"),
//      (6, "上海分行营业部"),
//      (7, "无锡分行营业部"),
//      (8, "南通分行营业部")
      (0, "六合区龙池地铁站"),
      (1, "南京鼓楼区长江路"),
      (2, "新街口"),
      (3, "紫峰大厦"),
      (4, "南京市长江大桥"),
      (5, "南京大润发"),
      (6, "北京地区"),
      (7, "南京市上海路99号")
    )).toDF("id", "flag")
  }

  def tfidf(data:DataFrame): DataFrame ={
    val dataSplit = udf{(words: String) => new JiebaSegmenter().sentenceProcess(words).toString}

    val splistRs=data.select("id","flag")
        .withColumn("text", col("flag"))
        .withColumn("flag", dataSplit(col("flag")))
        .withColumn("flag",regexp_replace(col("flag"),"[\\[\\]]",""))

    val regexTokenizer = new RegexTokenizer().setInputCol("flag").setOutputCol("words").setPattern("\\,")
    val regexTokenized = regexTokenizer.transform(splistRs)

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawfeatures").setNumFeatures(2000)
    val featurizedData = hashingTF.transform(regexTokenized)
    val idf = new IDF().setInputCol("rawfeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rsdata=idfModel.transform(featurizedData)
        .select(col("id"),col("text"),col("flag"),col("features"))

    return rsdata
  }
}
