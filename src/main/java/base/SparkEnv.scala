package base

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created byX on 2020-07-14 00:04
 * Desc:
 */
object SparkEnv {

  System.setProperty("hadoop.home.dir" ,"D:\\hadoop\\hadoop-2.7.7" )


  def getSc():SparkContext={
    val conf =new SparkConf().setAppName("sun").setMaster("local[2]")
    val sc=new SparkContext(conf)
    sc
  }


}
