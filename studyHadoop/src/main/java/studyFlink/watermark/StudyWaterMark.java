package studyFlink.watermark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class StudyWaterMark {
  private static SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static class Data {
    long date;
    String name;
    long num;

    public Data() {
    }

    @Override
    public String toString() {
      return "Data{" +
          "date=" + dateFormat.format(date) +
          ", name='" + name + '\'' +
          ", num=" + num +
          '}';
    }

    public Data(String name, long date) {
      this(name, date, 1);
    }

    public Data(String name, Long date, long num) {
      this.name = name;
      this.date = date;
      this.num = num;
    }

    public static Data str2Data(String str) {
      try {
        long date = dateFormat.parse(str.split(",")[1]).getTime();
        String name = str.split(",")[0];
        return new Data(name, date);
      } catch (ParseException e) {
        e.printStackTrace();
        return null;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);
    DataStream<String> text = env.socketTextStream("localhost", 9000);
    text.map(x -> Data.str2Data(x))
        .assignTimestampsAndWatermarks(new MyWaterMark())
        .keyBy(x -> x.name)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce((a, b) -> new Data(a.name, b.date, a.num + b.num))
        .print();

    env.execute();
  }

  private static class MyWaterMark implements AssignerWithPeriodicWatermarks<Data> {

    long maxOutOfOrder = 10000L;
    long currentMaxTimeStamp = 0L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
      return new Watermark(this.currentMaxTimeStamp - this.maxOutOfOrder);
    }

    @Override
    public long extractTimestamp(Data element, long previousElementTimestamp) {
      currentMaxTimeStamp = Math.max(currentMaxTimeStamp, element.date);
      System.out.println(String.format(
          "name: %s date: %s currentTimeStamp: %s  waterMark: %s",
          element.name,
          dateFormat.format(element.date),
          dateFormat.format(currentMaxTimeStamp),
          dateFormat.format(getCurrentWatermark().getTimestamp())));
      return currentMaxTimeStamp;
    }
  }
}
