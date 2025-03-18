import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

case class GenresRecommendation(genres: String, recs: Seq[Recommendation])


object StatisticsRecommend {
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  val RATE_MORE_MOVIES = "RateMoreMovies" 
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies" 
  val AVERAGE_MOVIES = "AverageMovies" 
  val GENRES_TOP_MOVIES = "GenresTopMovies" 

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommend")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    ratingDF.createOrReplaceTempView("ratings")

    val rateMoreMoviesDF = sparkSession.sql("select mid,count(mid) as count from ratings group by mid")
    storeInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)


    val simpleDateFormat = new SimpleDateFormat("yyyyMM");
 
    sparkSession.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
 
    val ratingOfYearMonth = sparkSession.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingsOfMonthYear")
   
    val rateMoreRecentlyMoviesDF = sparkSession.sql("select mid, count(mid) as count, yearmonth from ratingsOfMonthYear group by yearmonth,mid order by yearmonth desc, count desc")

    storeInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES);


    val averageMoviesDF = sparkSession.sql("select mid, avg(score) as avg from ratings group by mid")
    storeInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Famil y", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western")
  
    val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid"))

    val genresRDD = sparkSession.sparkContext.makeRDD(genres)

    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter {

        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }
      .map {
        case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
      }
      .groupByKey()
      .map {
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }
      .toDF()
    storeInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)
  }

  def storeInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}