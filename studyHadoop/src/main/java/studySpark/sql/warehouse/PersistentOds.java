package studySpark.sql.warehouse;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.collection.TraversableOnce;

import java.io.Serializable;

public class PersistentOds implements Serializable {

  public void run() {
    SparkSession spark = SparkSession.builder()
        .master("local[2]")
        .appName("persistent Ods log")
        .getOrCreate();
    Dataset<Row> rows = spark.read().text(getPath());
    Dataset<String> data = rows.map((MapFunction<Row, String>) row -> createJson(row.getString(0)), Encoders.STRING());
    Dataset<Row> dataFrame = spark.read().schema(getSchema()).json(data);
    dataFrame.printSchema();
    dataFrame.show();
    dataFrame.select(functions.explode_outer(functions.col("body"))).show();
//    dataFrame.select("body").select("source").select("app").show();
//        .select(col("app"))
  }

  private String getPath() {
    return getClass().getClassLoader().getResource("originalLog.txt").getPath();
  }

  private String createJson(String s) {
    int index = s.indexOf("[{");
    String body = s.substring(index);
    return "{\"body\":" + body + "}";
  }

  private StructType getSchema() {
    return DataTypes.createStructType(new StructField[] {
       DataTypes.createStructField("body", DataTypes.createArrayType(
           DataTypes.createStructType(new StructField[] {
              DataTypes.createStructField("source", DataTypes.createStructType(new StructField[] {
                  DataTypes.createStructField("app", DataTypes.StringType, true),
                  DataTypes.createStructField("mode", DataTypes.StringType, true)
              }), true)
           })
       ), true)
    });
  }

  public static void main(String[] args) {
    PersistentOds persistentOds = new PersistentOds();
    persistentOds.run();
  }
}
