package ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created byX on 2020-08-06 23:26
 * Desc:
 */
object PipelineLR {

  def noPipelineLR(spark:SparkSession)={
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")

    val lr=new LogisticRegression()
    println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")
    lr.setMaxIter(10).setRegParam(0.01)
    val model=lr.fit(training)
    println(s"Model 1 was fit using parameters: ${model.parent.extractParamMap}")
    val paramMap=ParamMap(lr.maxIter->20).put(lr.maxIter,30).put(lr.regParam->0.1,lr.threshold->0.55)
    val paramMap2=ParamMap(lr.probabilityCol->"myProbability")
    val paramMapCom=paramMap++paramMap2
    val newModel=lr.fit(training,paramMapCom)
    println(s"Model 2 was fit using parameters: ${newModel.parent.extractParamMap}")

    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    newModel.transform(test).show()
  }


  def pipelineLR(spark:SparkSession)= {
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    val model = pipeline.fit(training)
    model.write.overwrite().save("result/spark-logistic-regression-model")
    pipeline.write.overwrite().save("result/pipeline-lr-model")

    val sameModel = PipelineModel.load("result/spark-logistic-regression-model")

    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
  }

  def getSc(): SparkSession = {
    val sc = SparkSession.builder().master("local[2]").appName("statistics").getOrCreate();
    sc.sparkContext.setLogLevel("ERROR")
    sc
  }

  def main(args: Array[String]): Unit = {

    val spark=getSc
    noPipelineLR(spark)
    println("======分界线=======")
    pipelineLR(spark)
    spark.stop()
  }

}
