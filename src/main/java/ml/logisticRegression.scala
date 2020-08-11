package ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

/**
 * Created byX on 2020-07-21 00:14
 * Desc:
 */
object logisticRegression {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LinearSVCExample").master("local[2]").getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")
    val training = spark.read.format("libsvm").load("result/sample_libsvm_data.txt");
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8);
    val lrModel = lr.fit(training);

    val mlr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setFamily("multinomial");

    val mlrModel = mlr.fit(training);

    println(s"${mlrModel.coefficientMatrix}");
    println("===")
    println(f"${lrModel.interceptVector}");

    val trainingSummary=lrModel.binarySummary;
    val objectiveHistory=trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss=>println(loss
    ))

    val roc =trainingSummary.roc
    roc.show()
    spark.stop()
  }

}
