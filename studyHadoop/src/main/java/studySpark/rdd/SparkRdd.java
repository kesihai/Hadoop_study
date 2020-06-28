package studySpark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkRdd {
  public static final Logger LOG = LoggerFactory.getLogger(SparkRdd.class);

  public static void main(String[] args) {
    if (args.length < 1) {
      LOG.info("parameters should be: {inputFilePath}");
      return;
    }
    SparkConf conf = new SparkConf()
        .setAppName("sike test app");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> distFile = sc.textFile(args[0]);
    JavaRDD<Integer> lineLength = distFile.map(s -> s.length());
    int totalLength = lineLength.reduce((a, b) -> a + b);
    System.out.println("total length is : " + totalLength);
    sc.stop();
  }
}
/*

 */