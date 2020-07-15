package rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.Array.range
import scala.collection.mutable

/**
 * Created byX on 2020-07-14 00:15
 * Desc:
 */
class SunRDD(sc:SparkContext) extends RDD[Int](sc,Nil){

  override def compute(split: Partition, context: TaskContext): Iterator[Int] =  {
    //    context.addTaskCompletionListener(context=>{
    //      println("error!!")
    //    });
    val partition = split.asInstanceOf[SunPartition]

    //模拟blockManager获取数据块地址
    val index= MapManager.getBlockId(partition.index);

    range(index,index+1000,1).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until 10).map(i=>{
      new SunPartition(i)
    }).toArray
  }

}

private class SunPartition( start:Int) extends Partition{
  override def index: Int = start
}



object MapManager{
  val map=new mutable.HashMap[Int,Int]();
  (0 until 10).map(i=>{
    map.put(i,i*1000);
  })
  def getBlockId(i:Int): Int=map.get(i).head;


  def main(args: Array[String]): Unit = {
    val sc=base.SparkEnv.getSc();
    val rdd1=new SunRDD(sc).toJavaRDD();
    val rdd2=new SunRDD(sc).toJavaRDD();
    println(rdd1.union(rdd2).distinct().count())
    //10000
  }

}

