package studySpark.stream;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkStreamWordCount {
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
        .appName("javaStructuredWordCount")
        .getOrCreate();
    Dataset<Row> lines = spark.readStream().format("socket").option("host", "localhost").option("port", 9999).load();
    Dataset<String> words = lines.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>)x -> Arrays.asList(x.split(",")).iterator(), Encoders.STRING());
    
  }
}
