package studySpark.sql.warehouse;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import studySpark.sql.warehouse.po.LogItem;
import utils.CommonUtil;

import java.util.Iterator;

public class PersistentOdsRdd {
  public static void main(String[] args) {
    String path = CommonUtil.getPath("originalLog.txt");
    SparkSession spark = SparkSession.builder()
        .master("local[2]")
        .appName("persistentOdsRdd")
        .getOrCreate();
    Dataset<String> logs = spark.read().textFile(path);
    Dataset<LogItem> logItems = logs.flatMap(new FlatMapFunction<String, LogItem>() {
      @Override
      public Iterator<LogItem> call(String s) throws Exception {
        return CommonUtil.WareHouseLogUtil.string2List(s).iterator();
      }
    }, Encoders.bean(LogItem.class));
    logItems.show();
  }
}
