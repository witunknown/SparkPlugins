package partitioner

import base.SparkEnv
import org.apache.spark.Partitioner

import scala.collection.mutable

/**
 * Created byX on 2020-07-15 22:48
 * Desc:
 */
class UDFPartitioner(num :Integer) extends Partitioner{

  private var _hashMap=new mutable.HashMap[String,Int]();

     var i=num;
     while (i>0){
        _hashMap.put(i+"",i%2)
        i-=1
    }
//  _hashMap.foreach(println(_))

  override def numPartitions: Int = {
    _hashMap.size/4
  }

  override def getPartition(key: Any): Int = {
    if(_hashMap.contains(String.valueOf(key))==true){
      return  _hashMap.get(String.valueOf(key)).head
    }
    2
  }
}

object UDFPartitioner{
  def main(args: Array[String]): Unit = {
    var sc=SparkEnv.getSc();
    var rdd= sc.parallelize(Seq("1","2","3","4","5","6","7","8","9","10","11","12","13"));
    println("rdd partitions is:"+rdd.partitions.size)  //2
    rdd.toJavaRDD().mapToPair((_,1)).partitionBy(new UDFPartitioner(12)).saveAsTextFile("result");
    sc.stop();
  }
}
