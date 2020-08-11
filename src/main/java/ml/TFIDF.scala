package ml

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
 * Created byX on 2020-08-08 14:07
 * Desc:TF-IDF
 * TF:词频，一个词条在某一文档中的频率  IDF：逆文件频率，总文件数/包含该词条的文件，再取对数  TF-IDF=TF*IDF
 */
object TFIDF {

  def getSc(): SparkSession = {
    val sc = SparkSession.builder().master("local[2]").appName("statistics").getOrCreate();
    sc.sparkContext.setLogLevel("ERROR")
    sc
  }
  def main(args: Array[String]): Unit = {
    val spark=getSc()
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi Iam heard about Spark Spark"),
      (0.0, "Iam wish Java could use use classes"),
      (1.0, "Logistic regression models models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordData=tokenizer.transform(sentenceData)

//    wordData.select("sentence","words").show();
    val hashingTF=new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(1048576)

    val featureData=hashingTF.transform(wordData)
    //确认是否有碰撞
    featureData.select("words","rawFeatures").show(40,false);

    val idf=new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel=idf.fit(featureData)

    val rescaleData=idfModel.transform(featureData)

    rescaleData.select("label","features").show()
//    hashingTF.save("idf/hashing");

//    idfModel.save("idf/model")
  }

}
