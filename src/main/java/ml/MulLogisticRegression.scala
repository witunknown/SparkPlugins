package ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

/**
 * Created byX on 2020-08-04 23:58
 * Desc:
 */
object MulLogisticRegression {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MulLogisticRegression").master("local[2]").getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")
    val training=spark.read.format("libsvm").load("result/sample_multiclass_classification_data.txt")
    val rl=new LogisticRegression().setMaxIter(50).setRegParam(0.1).setElasticNetParam(0.5)

    //fit the model
    val lrModel=rl.fit(training)

    val trainingSummary=lrModel.summary;
    val objectiveHistory=trainingSummary.objectiveHistory
    println(f"objectiveHistory:")
    objectiveHistory.foreach(println)

    println("False positive rate by label:")
    trainingSummary.falsePositiveRateByLabel.zipWithIndex.foreach{case(rate,label)=>
          println(s"label $label: $rate")
    }
    println("True positive rate by label:")
    trainingSummary.truePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
      println(s"label $label: $rate")
    }
    println("Recall by label:")
    trainingSummary.recallByLabel.zipWithIndex.foreach { case (rec, label) =>
      println(s"label $label: $rec")
    }
    println("F-measure by label:")
    trainingSummary.fMeasureByLabel.zipWithIndex.foreach { case (f, label) =>
      println(s"label $label: $f")
    }


    /**
     *    正确（TP）:实时为true,预测为true;
     *   假正(FP)：实时为false,预测为true;
     *   假负（FN）:实时为true,预测为false;
     *   真负（TN）:实时为false,预测为false;
     */
    //准确率:accuracy=(TP+TN)/(TP+FP+FN+TN)
    val accuracy=trainingSummary.accuracy
    //假正(FN)：实时为false,预测为true;
    val falsePositiveRate=trainingSummary.weightedFalsePositiveRate
    //真正类(TP)：实时为true,预测为true;
    val truePositiveRate=trainingSummary.weightedTruePositiveRate

    //
    val fMeasure=trainingSummary.weightedFMeasure
    //精确率：precision=TP/(TP+FP)
    val precision=trainingSummary.weightedPrecision
    //召回率：recall=TP/(TP+FN)
    val recall=trainingSummary.weightedRecall

    println(s"Accuracy: $accuracy\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
      s"F-measure: $fMeasure\nPrecision: $precision\nRecall: $recall")


  }

}
