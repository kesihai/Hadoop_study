package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * study spark sql.
 */
public class StudySparkSql {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("study java spark sql")
        .getOrCreate();
    Dataset<Row> df = spark.read().json("resources/people.json");
    df.show();
  }
}
