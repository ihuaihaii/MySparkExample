import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object RDDExample {

  def RDDT_(sc:SparkContext): Unit ={

  }

  def RDDA_(sc:SparkContext): Unit ={

  }

  def main(args:Array[String]): Unit ={
    val sc=SparkSession.builder()
      .master("local")
      .appName("RDDTest")
      .getOrCreate()
      .sparkContext

    val rdd1=sc.parallelize(1 to 100,5)


  }
}
