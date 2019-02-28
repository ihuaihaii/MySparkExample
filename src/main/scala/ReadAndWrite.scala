import org.apache.spark.sql.SparkSession

object ReadAndWrite {

  def main(args:Array[String]): Unit ={

    //创建SparkSession实例
    val sks=SparkSession.builder().master("local").appName("ReadAndWrite").getOrCreate()

    /**
      * text格式数据读写
      * sks.read.text:读取text数据
      * 返回dataframe数据集
      * */
    val txtFile=sks.read.text("file:///D:\\project\\MySparkExample\\data\\graphx-wiki-vertices.txt")
    txtFile.show()

    /**
      * csv格式数据读写
      * 读取csv格式数据
      *
      *
      * */



    /**
      * json格式数据读写
      * 读取json格式数据
      *
      * */



    /**
      * jdbc格式数据读写
      * 读取jdbc的数据
      *
      * */



  }
}
