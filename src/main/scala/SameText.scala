import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer}
import org.apache.spark.sql.functions._
import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.ml.linalg.BLAS

object SameText {

  def main(args:Array[String]): Unit ={
    val spark=SparkSession.builder().appName("sameText").master("local").getOrCreate()
    val sqlContext=spark.sqlContext

    val sentenceData = sqlContext.createDataFrame(Seq(
             (0, "I heard about Spark and I love Spark"),
             (0, "I wish Java could use case classes"),
             (1, "Logistic regression models are neat")
             )).toDF("id", "flag")

    val sentenceData1 = sqlContext.createDataFrame(Seq(
      (1, "南京银行鼓楼分行"),
      (2, "鼓楼分行"),
      (3, "南京银行鼓楼分行鼓楼支行")
    )).toDF("id", "flag")

    val dataSplit = udf{(words: String) => new JiebaSegmenter().sentenceProcess(words).toString}

    val splistRs=sentenceData1.select("id","flag")
      .withColumn("flag", dataSplit(col("flag")))
      .withColumn("flag",regexp_replace(col("flag"),"[\\[\\]]",""))

    val regexTokenizer = new RegexTokenizer().setInputCol("flag").setOutputCol("words").setPattern("\\,")
    val regexTokenized = regexTokenizer.transform(splistRs)

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawfeatures")
    val featurizedData = hashingTF.transform(regexTokenized)

    val idf = new IDF().setInputCol("rawfeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val aa = idfModel.transform(featurizedData).select(col("id"),col("flag"),col("features"))
    featurizedData.show(false)
    aa.show(false)
    aa.printSchema()
    aa.select(col("features")).show(false)
  }
}
