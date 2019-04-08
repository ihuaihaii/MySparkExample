
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.ml.fpm.{FPGrowth=>FPGrowth2}

object MllibTest {

  def FPgrowthTest(sks:SparkSession){
    val data = sks.sparkContext.textFile("data/sample_fpgrowth.txt")
    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))
    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }

  def FPgrowthTest2(sks:SparkSession){
    val data = sks.read.text("data/sample_fpgrowth.txt")

    import sks.implicits._

    val transactions=data.map(_.toString.split(' '))

    val fpg = new FPGrowth2()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.fit(transactions)

//    model.freqItemsets.collect().foreach { itemset =>
//      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
//    }
//
//    val minConfidence = 0.8
//    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
//      println(
//        rule.antecedent.mkString("[", ",", "]")
//          + " => " + rule.consequent .mkString("[", ",", "]")
//          + ", " + rule.confidence)
//    }
  }

  def main(args:Array[String]): Unit ={
    val sks=SparkSession.builder().master("local").appName("mllibTest").getOrCreate()
    FPgrowthTest(sks)
  }
}
