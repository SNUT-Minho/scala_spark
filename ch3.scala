package scala_spark
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ ALS, ALSModel }
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.broadcast._
object RunRecommender extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("aa").getOrCreate();
    //val conf = new SparkConf().setMaster("local[*]").setAppName("aa")
    //val sc = new SparkContext(conf)
    import spark.implicits._
    val base = "C:/Users/MinhoLee/Desktop/ch3/"
    val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
    val rawArtistData = spark.read.textFile(base + "artist_data.txt")
    val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")
   
    
    val userArtistDF = rawUserArtistData.map { line =>
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }.toDF("user", "artist")

    //userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()
  
 
    val artistByID = rawArtistData.flatMap {
      line =>
        val (id, name) = line.span(_ != '\t')
        if (name.isEmpty) {
          None
        } else {
          try {
            Some((id.toInt, name.trim))
          } catch {
            case _: NumberFormatException => None
          }
        }
    }.toDF("id", "name")
    

    val artistAlias = rawArtistAlias.flatMap { line =>
      val Array(artist, alis) = line.split('\t')
      if (artist.isEmpty) {
        None
      } else {
        Some((artist.toInt, alis.toInt))
      }
    }.collect().toMap
    
    //artistByID.filter($"id" isin (1208690, 1003926)).show()

    val bArtistAlias = spark.sparkContext.broadcast(artistAlias)
    
  
    def buildCounts(
      rawUserArtistData: Dataset[String],
      bArtistAlias: Broadcast[scala.collection.immutable.Map[Int, Int]]): DataFrame = {
      rawUserArtistData.map { line =>
        val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
        val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
        (userID, finalArtistID, count)
      }.toDF("user", "artist", "count")
    }
    
    val trainData = buildCounts(rawUserArtistData, bArtistAlias).cache()
    
    import org.apache.spark.ml.recommendation._
    import scala.util.Random
    
    // 무작위 초기값을 적용한 ALS 객체
    val model = new ALS().
                setSeed(Random.nextLong()).
                setImplicitPrefs(true).
                setRank(10).
                setRegParam(0.01).
                setAlpha(1.0).
                setMaxIter(5).
                setUserCol("user").
                setItemCol("artist").
                setRatingCol("count").
                setPredictionCol("prediction").  
                fit(trainData)
    
   
    trainData.unpersist()            
    model.userFactors.show(1, truncate = false)
    
    val userID = 2093760
    
    val existingArtistIDs = trainData.filter($"user" === userID).select("artist").as[Int].collect()
    
    artistByID.filter($"id" isin (existingArtistIDs:_*)).show()
    
    def makeRecommendations(model: ALSModel, userID: Int, howMany: Int): DataFrame = {
      val toRecommend = model.itemFactors.
      select($"id".as("artist")).
      withColumn("user", lit(userID))
      
      model.transform(toRecommend).
      select("artist","prediction").
      orderBy($"prediction".desc).
      limit(howMany)
      
    }
    
    val topRecommendations = makeRecommendations(model, userID, 5)
    topRecommendations.show()
    
  }
  
  
  class RunRecommender(private val spark: SparkSession) {
   
  }
}