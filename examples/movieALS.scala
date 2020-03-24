import scala.collection.mutable

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import sys.process._

//load local data to hdfs
"hdfs dfs -put data/sample_movielens_data.txt /tmp" !

//configurable parameters
val input = "/tmp/sample_movielens_data.txt" //needs to match location in hdfs
val kryo = false

//ALS implementation parameters:
val numIterations = 5
val lambda = 1.0
val rank = 10
val numUserBlocks = -1
val numProductBlocks = -1
val implicitPrefs = false

/* Compute RMSE (Root Mean Squared Error). */
def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean): Double = {
  def mapPredictedRating(r: Double): Double = {
    if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
  }
  val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
  val predictionsAndRatings = predictions.map{ x =>
    ((x.user, x.product), mapPredictedRating(x.rating))
  }.join(data.map(x => ((x.user, x.product), x.rating))).values
  math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
}

val ratings = sc.textFile(input).map { line =>
      val fields = line.split("::")
      if (implicitPrefs) {
        /*
         * MovieLens ratings are on a scale of 1-5:
         * 5: Must see
         * 4: Will enjoy
         * 3: It's okay
         * 2: Fairly bad
         * 1: Awful
         * So we should not recommend a movie if the predicted rating is less than 3.
         * To map ratings to confidence scores, we use
         * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
         * entries are generally between It's okay and Fairly bad.
         * The semantics of 0 in this expanded world of non-positive weights
         * are "the same as never having interacted at all".
         */
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
      } else {
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    }.cache()

val numRatings = ratings.count()
val numUsers = ratings.map(_.user).distinct().count()
val numMovies = ratings.map(_.product).distinct().count()

println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

//let's create a training and testing
val splits = ratings.randomSplit(Array(0.8, 0.2))
val training = splits(0).cache()

val test = if (implicitPrefs) {
  /*
   * 0 means "don't know" and positive values mean "confident that the prediction should be 1".
   * Negative values means "confident that the prediction should be 0".
   * We have in this case used some kind of weighted RMSE. The weight is the absolute value of
   * the confidence. The error is the difference between prediction and either 1 or 0,
   * depending on whether r is positive or negative.
   */
  splits(1).map(x => Rating(x.user, x.product, if (x.rating > 0) 1.0 else 0.0))
} else {
  splits(1)
}.cache()

val numTraining = training.count()
val numTest = test.count()
println(s"Training: $numTraining, test: $numTest.")

//have split the rating into two separate cached sets, can free up this space
ratings.unpersist(blocking = false)

//use trainImplicit for implicit data
val model = ALS.train(training, rank, numIterations, 0.01)

//how accurate is our model?
val rmse = computeRmse(model, test, implicitPrefs)

println(s"Test RMSE = $rmse.")

//to get a prediction, enter the user (id) and movie (id) to get a predicted rating.
model.predict(3,51)
