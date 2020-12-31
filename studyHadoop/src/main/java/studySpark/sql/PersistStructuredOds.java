package studySpark.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class PersistStructuredOds {
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
        .appName("PersistStructuredOds")
        .master("local[2]")
        .getOrCreate();

    Dataset<String> logs = spark.read().textFile("studyHadoop/src/main/resources/originalLog.txt");
    Dataset<String> mergeSet = logs.map((MapFunction<String, String>)(x -> OdsHelper.mergeHeader(x)), Encoders.STRING());
    spark.read().schema()
  }
}

class OdsHelper {
  public static String mergeHeader(String log) {
    int index = log.indexOf("[{");
    String header = log.substring(index);
    String body = log.substring(0, index);
    return "{\"header\":\"" + header + "\",\"body\":" + body + "}";
  }

  public static StructType getStructType() {
    List<StructField> list = new LinkedList<>();
    list.add(DataTypes.createStructField("header", DataTypes.StringType, true));
    list.add(DataTypes.createStructField("body", DataTypes.createStructType(
        Arrays.asList(DataTypes.createStructField("app", DataTypes.StringType, true),
            )
    )));
  }
}

