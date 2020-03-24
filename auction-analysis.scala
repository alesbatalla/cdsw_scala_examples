/*
Let's play with some ebay auction data
*/

//load local data to hdfs
import sys.process._
"hdfs dfs -put data/ebay-xbox.csv /tmp" !

// SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

// Import Spark SQL data types, row and math functions
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// load the data into a new RDD
val ebayText = sc.textFile("/tmp/ebay-xbox.csv") // needs to match location in hdfs
// Return the first element in this RDD
ebayText.first()

//define the schema using a case class
case class Auction( auctionid: String,
                    bid: Float,
                    bidtime: Float,
                    bidder: String,
                    bidderrate: Integer,
                    openbid: Float,
                    price: Float,
                    item: String )

// create an RDD of Auction objects
val ebayRDD = ebayText.map(_.split(",")).map(p => Auction(p(0),
 p(1).toFloat,p(2).toFloat,p(3),p(4).toInt,p(5).toFloat, p(6).toFloat, "xbox"))

// Return the number of elements in the RDD
ebayRDD.count()

// now let's try working with a DataFrame
val ebayDF = ebayRDD.toDF()

// Display the top 20 rows of the DataFrame
ebayDF.show()
// Return the schema of this DataFrame
ebayDF.printSchema()

// let's debug the physical plan for auctionId just for kicks
ebayDF.select("auctionid").distinct.explain()

/* Time to use this dataframe to answer some questions */

// How many auctions were held?
ebayDF.select("auctionid").distinct.count
// How many bids per item?
ebayDF.groupBy("auctionid", "item").count.show

// What's the min number of bids per item?
// what's the average? what's the max?
ebayDF.groupBy("item", "auctionid").count.agg(min("count"), avg("count"),max("count")).show

val smallset = ebayDF.filter("auctionid = 8212590929")
smallset.show()
// Get the auctions with closing price > 100
val highprice= ebayDF.filter("price > 100")

// display dataframe in a tabular format
highprice.show()
