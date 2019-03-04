import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RDDExample {

  def RDDT_Map(sc:SparkContext): Unit ={

    //sc.parallelize(1 to 100,5)表示将1 to 100产生的集合（Range）转换成一个RDD
    // //并创建5个partition

    val rdd1=sc.parallelize(1 to 100,5)

    rdd1.map(line=>line+100).collect().foreach(println)

  }

  def RDDA_(sc:SparkContext): Unit ={

  }

  def main(args:Array[String]): Unit ={
    val sc=SparkSession.builder()
      .master("local")
      .appName("RDDTest")
      .getOrCreate()
      .sparkContext

    RDDT_Map(sc)

  }
}
