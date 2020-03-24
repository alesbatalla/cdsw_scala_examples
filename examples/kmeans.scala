import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.io.FileUtils
import java.io.File
import sys.process._

//load local data to hdfs
"hdfs dfs -put data/kmeans_data.txt /tmp" !

//example kmeans clustering script
val data = sc.textFile("/tmp/kmeans_data.txt")
val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

// Cluster the training data set into two classes with KMeans
val numClusters = 2
val numIterations = 20
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
println(s"Within Set Sum of Squared Errors = $WSSSE")

// Save the model
val output ="output/KMeansExample/KMeansModel"
FileUtils.deleteQuietly(new File(output))
clusters.save(sc, output)

//example of loading and predicting on the model we created
val sameModel = KMeansModel.load(sc, output)
sameModel.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
  println(s"Cluster Center ${idx}: ${center}")
}
sameModel.predict(Vectors.dense(7,5,6))
