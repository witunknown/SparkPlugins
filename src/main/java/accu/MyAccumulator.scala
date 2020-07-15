package accu

import base.SparkEnv
import org.apache.spark.util.AccumulatorV2


/**
 * Created byX on 2020-07-14 23:51
 * Desc:
 */
class MyAccumulator extends AccumulatorV2[(String, Long),(Integer, Long)]{

  private var accumulator = new Tuple2[Integer,Long](0,0L);


  //累加器是否为空
  override def isZero: Boolean = {
        accumulator._2==0L&&accumulator._1==0
  }

  //复制一份累加器对象至其他分区
  override def copy(): AccumulatorV2[(String, Long), (Integer, Long)] = {
      new MyAccumulator()
  }
  //重置累加器对象
  override def reset(): Unit = {
        accumulator=new Tuple2[Integer,Long](0,0L);
  }

  //分区内相加
  override def add(v: (String, Long)): Unit = {
    val key=accumulator._1+v._1.length();
    val value=accumulator._2+v._2;
    accumulator=new Tuple2[Integer,Long](key,value);
  }

  //分区数据合并
  override def merge(other: AccumulatorV2[(String, Long), (Integer, Long)]): Unit = {
    accumulator=new Tuple2[Integer,Long](accumulator._1+other.value._1,accumulator._2+other.value._2);
  }

  //累加器结果
  override def value: (Integer, Long) = accumulator
}

object MyAccumulator{
  def main(args: Array[String]): Unit = {
    //spark环境
    val sc=SparkEnv.getSc();
    //创建累加器
    val  myAccu=new MyAccumulator
    //注册
    sc.register(myAccu)
    sc.parallelize(Array(("a",4L),("a",3L),("a",2L),("a",1L))).foreach(myAccu.add)
    println(myAccu.accumulator._1+":"+myAccu.accumulator._2);
    //4:10
    sc.stop()
  }
}
