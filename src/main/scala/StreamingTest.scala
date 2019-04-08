import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession


object StreamingTest{

  def main(args:Array[String]): Unit ={

    val ss=SparkSession.builder().master("local").appName("streaming test").getOrCreate()

    val sting=new StreamingContext(new SparkConf(),Seconds(1))

    println("spark streaming test")

  }
}
