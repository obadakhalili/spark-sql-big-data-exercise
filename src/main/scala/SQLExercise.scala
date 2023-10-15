import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SQLExercise {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(SQLExercise.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    // Task 1: Split students into classes by studying status and count the number of students in each class
    val df = spark.read
      .csv("data/students.csv")
      .toDF("student_id", "first_name", "last_name", "birth_date", "gpa", "studying_status")

    val classCounts = df.groupBy("studying_status").count()
    classCounts.show()

    // Task 2: View student first name, last name, and GPA for students who have graduated
    val graduatedStudents = df.filter(col("studying_status") === "graduated")
    graduatedStudents.select("first_name", "last_name", "gpa").show()

    // Task 3: Calculate the mean of GPAs for students who are studying
    val studyingGPA = df.filter(col("studying_status") === "studying").agg(avg("gpa").alias("avg_gpa"))
    studyingGPA.show()

    // Task 4: Sort students by GPA in descending order
    val sortedByGPA = df.orderBy(col("gpa").desc)
    sortedByGPA.show()

    // Task 5: Add a new column 'age' calculated from birth date
    val dfWithAge = df.withColumn("age", year(current_date()) - year(col("birth_date")))
    dfWithAge.show()

    // Task 6: Show only the students who have a GPA greater than 3.7
    val highGPAS = df.filter(col("gpa") > 3.7)
    highGPAS.show()

    spark.stop()
  }
}
