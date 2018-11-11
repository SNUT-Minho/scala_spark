package scala_spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{ DataFrame, Dataset, Row, SparkSession }

case class MatchData(
  id_1: Int,
  id_2: Int,
  cmp_fname_c1: Option[Double],
  cmp_fname_c2: Option[Double],
  cmp_lname_c1: Option[Double],
  cmp_lname_c2: Option[Double],
  cmp_sex: Option[Int],
  cmp_bd: Option[Int],
  cmp_bm: Option[Int],
  cmp_by: Option[Int],
  cmp_plz: Option[Int],
  is_match: Boolean)

object ch2 extends Serializable {
  // 데이터셋에 대한 데이터프레임을 생성하기 위해서 사용하는 객체

  /*
  Both map and reduce have as input the array and a function you define.
  They are in some way complementary: map cannot return one single element for an array of multiple elements,
  while reduce will always return the accumulator you eventually changed.
  */

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("aa").getOrCreate();
    import spark.implicits._
    //val conf = new SparkConf().setMaster("local[*]").setAppName("aa");
    //val sc = new SparkContext(conf)
    //val ssc = new SQLContext(sc)
    //import ssc.implicits._

    //val rdd = sc.parallelize(Array(1, 2, 2, 4), 4)
    //print(rdd.first())

    //val rawblocks = sc.textFile("C:/Users/MinhoLee/eclipse-workspace/spark_ex/linkage/*")
    //val head = rawblocks.take(10)
    //head.foreach(println)

    def isHeader(line: String): Boolean = {
      line.contains("id_1")
    }

    //spark Session
    
    //head.filterNot(isHeader).foreach(println)
    //head.filter(x => !isHeader(x)).foreach(println)
   

    // inferSchema : 데이터 타입 변
    val prev = spark.read.csv("linkage/*")
    val parsed = spark.read.option("header", "true").option("nullValue", "?").option("inferSchema", "true").csv("linkage/*")
    //parsed.show()
    //parsed.printSchema()
    parsed.cache()

    // 기존 RDD API를 사용한 방법
    //parsed.rdd.map(_.getAs[Boolean]("is_match")).countByValue()

    // 최신 Dataframe API를 이용한 방법
    //parsed.groupBy("is_match").count().orderBy("count").show()

    // 데이터프레임 집계 함수
    //parsed.agg(functions.avg("cmp_sex"), functions.stddev("cmp_sex")).show()

    // Spark SQL 실행 엔진을 parsed 데이터프레임과 연결
    parsed.createOrReplaceTempView("linkage")

    // Spark 임시테이블에 SQL 질의문을 활용해 데이터 추출
    /*
    spark.sql("""
    SELECT is_match, Count(*) cnt
    FROM linkage
    GROUP BY is_match
    ORDER BY cnt DESC
    """).show()
    */

    // DataframeAPI를 사용한 빠른 요약 통계
    val summary = parsed.describe()
    //summary.show()
    //summary.select("sumary","cmp_fname_c1","cmp_fname_c2").show()

    // is_mach 열의 값의 상관관계에 대한 요약 통계
    // SQL 스타일
    val matches = parsed.where("is_match = true")
    val matchSummary = matches.describe()

    // DataFrame API의 Column 객체 형식
    val misses = parsed.filter($"is_match" === false)
    val missSummary = misses.describe()

    // DataFrame의 Schema 객체
    //summary.printSchema()

    val schema = summary.schema
    // longForm의 결과값은 Dataset[T]라는 사실에 주의하자.
    val longForm = summary.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => {
        (metric, schema(i).name, row.getString(i).toDouble)
      })
    })

    val longDF = longForm.toDF("metric", "field", "value")
    //longDF.show()

    val wideDF = longDF.groupBy("field").
      pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
      agg(first("value"))
    //wideDF.select("field","count","mean").show()

    // pivotSummart function
    def pivotSummary(desc: DataFrame): DataFrame = {
      val schema = desc.schema
      import desc.sparkSession.implicits._

      val lf = desc.flatMap(row => {
        val metric = row.getString(0)
        (1 until row.size).map(i => {
          (metric, schema(i).name, row.getString(i).toDouble)
        })
      }).toDF("metric", "field", "value")

      lf.groupBy("field").
        pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
        agg(first("value"))

    }

    val matchSummaryT = pivotSummary(matchSummary)
    val missSummaryT = pivotSummary(missSummary)

    matchSummaryT.createOrReplaceTempView("match_desc")
    missSummaryT.createOrReplaceTempView("miss_desc")
    
    /*
    spark.sql("""
    SELECT a.field, a.count + b.count as total, a.mean - b.mean as delta
    FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field 
    WHERE a.field NOT IN ("id_1", "id_2")
    ORDER BY delta DESC, total DESC
    """).show()
		*/

    val matchData = parsed.as[MatchData]
    matchData.show()

    case class Score(value: Double) {
      def +(oi: Option[Int]) = {
        Score(value + oi.getOrElse(0))
      }
    }

    def scoreMatchData(md: MatchData): Double = {
      (Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz + md.cmp_by + md.cmp_bd + md.cmp_bm).value
    }

    
    val scored = matchData.map {
       md => (scoreMatchData(md), md.is_match)
    }.toDF("score","is_match")
  	
    
    def crossTabs(scored: DataFrame, t: Double) : DataFrame = {
       scored.selectExpr(s"score >= $t as above", "is_match").groupBy("above").pivot("is_match", Seq("true","false")).count()
    }
    
    crossTabs(scored, 4.0).show()
  }
}