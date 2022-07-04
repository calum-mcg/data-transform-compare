package stackoverflow
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, LogManager}

object AggregateUsers {
  def main(args: Array[String]): Unit = {
    // Start time
    val start_time = System.nanoTime

    // Specify gcs bucket
    val gcsBucket = "example-gcs-bucket"

    // Create logger
    val log = LogManager.getRootLogger  
    log.setLevel(Level.INFO) 

    // Create Spark session
    val spark = SparkSession
        .builder()
        .appName("Aggregate Stackoverflow Users")
        .config("spark.master", "local")
        .config("spark.sql.broadcastTimeout", "36000")
        .getOrCreate()
    // Allow for implicit conversions between Scala objects
    spark.implicits

    // Set types/schema for each dataset
    val postSchema = StructType(Array(
      StructField("post_id", IntegerType, true),
      StructField("title", StringType, true),
      StructField("body_text", StringType, true),
      StructField("score", IntegerType, true),
      StructField("view_count", DoubleType, true), // Double as nulled if Integer due to source data
      StructField("comment_count", IntegerType, true), 
      StructField("user_poster_id", DoubleType, true)) // Double as nulled if Integer due to source data
    )

    val userSchema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("displayname", StringType, true),
      StructField("reputation", IntegerType, true),
      StructField("aboutme", StringType, true),
      StructField("websiteurl", StringType, true),
      StructField("location", StringType, true),
      StructField("profileimageurl", StringType, true),
      StructField("views", IntegerType, true),
      StructField("upvotes", IntegerType, true),
      StructField("downvotes", IntegerType, true))
    )

    val countriesSchema = StructType(Array(
      StructField("name", StringType, true),
      StructField("code", StringType, true))
    )

    // Load files from GCS into dataframes
    log.info("Loading data from GCS...")
    val postsDf = spark.read.option("header", "true").schema(postSchema).csv("gs://" + gcsBucket + "/posts.csv.gz")
    val usersDf = spark.read.option("header", "true").schema(userSchema).csv("gs://" + gcsBucket + "/users.csv.gz")
    var countryCodeDf = spark.read.option("header", "true").schema(countriesSchema).csv("gs://" + gcsBucket + "/country_codes.csv")
    log.info("Transforming data...")
    // Group by user_id and create aggregate columns
    // Rounding to the nearest 4 dp
    var aggregatedPostsByUser = postsDf.groupBy("user_poster_id")
      .agg(
        (when((countDistinct("post_id")/sum("comment_count")).isNull, 0).otherwise(round((countDistinct("post_id")/sum("comment_count")), 4))).as("post_to_comment_ratio"),
        round(avg("score"), 4).as("average_post_score")
      )

    // Add last updated column
    aggregatedPostsByUser = aggregatedPostsByUser.withColumn("last_updated", current_timestamp())

    // Left join with users
    aggregatedPostsByUser = aggregatedPostsByUser.join(usersDf,aggregatedPostsByUser("user_poster_id") ===  usersDf("user_id"), "left")
    .select("user_poster_id", "displayname", "post_to_comment_ratio", "average_post_score", "location", "last_updated")
    .withColumnRenamed("user_poster_id", "user_id")

    // Calculate country column in aggregated dataframe
    aggregatedPostsByUser = aggregatedPostsByUser.withColumn("country",trim(lower(regexp_extract(col("location"),"([^,]+$)", 0))))
    // Ensure corresponding country column is sanitised pre-join
    countryCodeDf = countryCodeDf.withColumn("name",trim(lower(col("name"))))

    // Perform inner join to get ISO 2-digit code
    aggregatedPostsByUser =  aggregatedPostsByUser.join(countryCodeDf, aggregatedPostsByUser("country") ===  countryCodeDf("name"), "inner")
    .select("user_id", "displayname", "post_to_comment_ratio", "average_post_score", "location", "code", "last_updated")

    // Sort by post_to_comment_ratio desc
    aggregatedPostsByUser = aggregatedPostsByUser.orderBy(desc("post_to_comment_ratio"))

    log.info("Uploading to GCS...")
    // Save to GCS (compressed)
    aggregatedPostsByUser.write.format("com.databricks.spark.csv").option("codec", "gzip").option("header", "true").save("gs://" + gcsBucket + "/spark/agg_users.csv.gz")

    // Calculate duration and log
    val duration = (System.nanoTime - start_time) / 1e9d
    log.info("Time taken:"+duration+"s")
  }
}