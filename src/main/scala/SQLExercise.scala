import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLExercise {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(SQLExercise.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    val df = spark
      .read
      .csv("data/students.csv")

    df.show(10)

    spark.stop()
  }
}
