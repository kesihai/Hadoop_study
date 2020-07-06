package studySpark.sql.hive;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unix_timestamp;

public class HiveData {
  private final static String table = "dws.test";
  private final static Logger LOG =
      LoggerFactory.getLogger(HiveData.class);

  @Data
  @AllArgsConstructor
  public static class Item {
    int id;
    long timeStamp;
  }

  public static void main(String[] args) {
  String str = "2020-05-20";
    SparkSession session = SparkSession
        .builder()
        .appName("sync user gender and signup info")
        .master("local[2]")
        .enableHiveSupport()
        .getOrCreate();
    String q = String.format("select * from %s where dt = '%s'", table, str);
    LOG.info(q);
    Dataset<Row> dataset = session.table(table).where(col("dt").equalTo(str));
    Dataset<Item> set =
        dataset.map(new MapFunction<Row, Item>() {
          @Override
          public Item call(Row row) throws Exception {
            return new Item(row.getAs("id"),
                row.<Date>getAs("createtime").getTime());
          }
        }, Encoders.bean(Item.class));
    set.show();
    session.close();
  }
}
