import org.apache.spark.sql.SparkSession

object DFAndDS {

  def main(args:Array[String]): Unit ={

    val sks=SparkSession.builder().master("local").appName("DFAndDS").getOrCreate()



  }
}
