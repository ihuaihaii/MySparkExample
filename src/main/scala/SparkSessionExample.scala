import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionExample {
  def main(args:Array[String]): Unit ={

    //方式一

    /**
      *
      * master("local") ：设置spark master地址
      * appName("SparkSessionExample")
      * */

    val sks=SparkSession.builder()
      .master("local")
      .appName("SparkSessionExample")
      .getOrCreate()

    /**
      * SparkSession:声明后，可以使用.conf.set方法设置参数
      * */
    sks.conf.set("spark.sql.shuffle.partitions", 6)
    sks.conf.set("spark.executor.memory", "2g")


    //方式二

    val sparkConf = new SparkConf

    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[3]")
    }

    if (!sparkConf.contains("spark.app.name")) {
      sparkConf.setAppName("MastGlobalDataProcessing-" + getClass.getName)
    }

    /***
      *
      * config(sparkConf)  ：将配置事先封装到sparkconf对象
      * enableHiveSupport():使SparkSession实例，可以执行sql，如：sks2.sql("").show
      ***/

    val sks2=SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    sks2.sql("").show

  }
}
