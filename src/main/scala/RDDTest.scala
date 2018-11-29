import org.apache.spark.sql.SparkSession

object RDDTest {
  def main(args:Array[String]): Unit ={
    SparkSession.builder().master("local").appName("RDDTest").getOrCreate()


  }
}
