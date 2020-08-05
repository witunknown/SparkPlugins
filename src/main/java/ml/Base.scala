package ml

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

/**
 * Created byX on 2020-07-23 00:17
 * Desc:
 */
object Base {

  def main(args: Array[String]): Unit = {
    val base = new Base;
//    base.func1()
//    base.func2
    base.func3
  }

}


class Base {

  def getSc(): SparkSession = {
    val sc = SparkSession.builder().master("local[2]").appName("statistics").getOrCreate();
    sc.sparkContext.setLogLevel("ERROR")
    sc
  }


  def func1(): Unit = {
    val data = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0)),
    )
    val sc = getSc();
    import sc.implicits._
    val df = data.toDF("label", "features");
    val chi = ChiSquareTest.test(df, "features", "label").head;
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
  }


  def func2: Unit = {
    val sc = SparkSession.builder().master("local[2]").appName("statistics").getOrCreate();
    val x1 = sc.sparkContext.parallelize(Array(1.0, 2.0, 3.0, 4.0));
    val y1 = sc.sparkContext.parallelize(Array(5.0, 6.0, 7.0, 8.0));
    val corr = Statistics.corr(x1, y1, "pearson");
    println(corr);
  }

  def func3: Unit = {
    val sc = getSc()
    val r2 = sc.sparkContext.parallelize(Array(Array(1.0, 2.0, 3.0, 4.0), Array(2.0, 3.0, 4.0, 5.0), Array(3.0, 4.0, 5.0, 6.0)))
    val rdd3 = r2.map(f => Vectors.dense(f))

//    val RM= new RowMatrix(rdd3,3,3)
  }


}