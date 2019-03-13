import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MllibTest {

  def FPgrowthTest(sks:SparkSession){
    val data = sks.sparkContext.textFile("data/mllib/sample_fpgrowth.txt")
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


  def mian(args:Array[String]): Unit ={
    val sks=SparkSession.builder().master("local").appName("mllibTest").getOrCreate()
    FPgrowthTest(sks)
  }


}
