package ml

import org.apache.spark.ml.feature.{Binarizer, CountVectorizer, CountVectorizerModel, FeatureHasher, NGram, PCA, RegexTokenizer, Tokenizer, Word2Vec}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions._
/**
 * Created byX on 2020-08-09 00:14
 * Desc:
 */
object Transform {

  def getSc(): SparkSession = {
    val sc = SparkSession.builder().master("local[2]").appName("statistics").getOrCreate();
    sc.sparkContext.setLogLevel("ERROR")
    sc
  }

  def word2Vec(spark:SparkSession)={
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(5)
      .setMinCount(0)
    val model=word2Vec.fit(documentDF)

    val result=model.transform(documentDF)

    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }
  }

  def countVectorizer(spark:SparkSession)={
    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).show(false)
  }

  def featureHasher(spark:SparkSession)={
    val dataset = spark.createDataFrame(Seq(
      (2.2, true, "1", "foo"),
      (3.3, false, "2", "bar"),
      (4.4, false, "3", "baz"),
      (5.5, false, "4", "foo")
    )).toDF("real", "bool", "stringNum", "string")

    val hasher = new FeatureHasher()
      .setInputCols("real", "bool", "stringNum", "string")
      .setOutputCol("features")

    val featurized = hasher.transform(dataset)
    featurized.show(false)
  }

  //Transformers
  def Tokenizer(spark:SparkSession)={
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

    val tokenizer=new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer =new RegexTokenizer().setInputCol("sentence").setOutputCol("words").setPattern("\\w")

    val countTokens= udf {words:Seq[String]=>words.length}

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("sentence", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("sentence", "words")
      .withColumn("tokens", countTokens(col("words"))).show(false)
  }

  /**
   * desc:
   * @param spark
   */
  def nGram(spark:SparkSession)={
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(3).setInputCol("words").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.select("ngrams").show(false)
  }

  /**
   * desc:0/1化
   * @param spark
   */
  def binarizer(spark:SparkSession)={
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)

    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binarizedDataFrame.show()
  }

  /**
   * pca降维度
   * @param spark
   */
  def pca(spark:SparkSession)={
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)

    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)
  }

  def main(args: Array[String]): Unit = {

    val spark=getSc();
//    word2Vec(spark)
//    countVectorizer(spark)
//    featureHasher(spark)
//    Tokenizer(spark)
//    nGram(spark)
//    binarizer(spark)
    pca(spark)
    spark.stop()

  }


}
