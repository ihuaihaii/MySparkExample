import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MllibTest {

  def FPgrowthTest(sks:SparkSession){

  }


  def mian(args:Array[String]): Unit ={
    val sks=SparkSession.builder().master("local").appName("mllibTest").getOrCreate()
    FPgrowthTest(sks)
  }


}
