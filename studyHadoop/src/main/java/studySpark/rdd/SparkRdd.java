package studySpark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkRdd {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setAppName("sike test app")
        .setMaster("local[3]")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", "/home/sike/src/testData/SparkEvents")
        .set("spark.history.fs.logDirectory", "/home/sike/src/testData/SparkEvents");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> distFile = sc.textFile("/home/sike/src/testData/a.txt");
    JavaRDD<Integer> lineLength = distFile.map(s -> s.length());
    int totalLength = lineLength.reduce((a, b) -> a + b);
    System.out.println("total length is : " + totalLength);
    sc.stop();
  }
}