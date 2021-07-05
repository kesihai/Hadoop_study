package studySpark.stream;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class SparkStreamWordCount {
  public static void main(String[] args) throws StreamingQueryException {
    SparkSession spark = SparkSession.builder()
        .master("local[2]")
        .appName("javaStructuredWordCount")
        .getOrCreate();
    Dataset<Row> lines = spark.readStream().format("socket").option("host", "localhost").option("port", 9999).load();
    Dataset<String> words = lines.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>)x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
//    Dataset<Row> wordCounts = words.groupBy("value").count();
    Dataset<Integer> wordCounts = words.groupBy("value").count().as(Encoders.INT());
    StreamingQuery query = wordCounts.writeStream()
        .format("console")
        .outputMode(OutputMode.Complete())
        .start();
    query.awaitTermination();
  }
}
