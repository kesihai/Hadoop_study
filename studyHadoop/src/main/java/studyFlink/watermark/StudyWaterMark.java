package studyFlink.watermark;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class StudyWaterMark {
  private static SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static class Data {
    int index;
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

    public Data(int index, String name, long date) {
      this(index, name, date, 1);
    }

    public Data(int index, String name, Long date, long num) {
      this.index = index;
      this.name = name;
      this.date = date;
      this.num = num;
    }

    public static Data str2Data(String str) {
      try {
        long date = dateFormat.parse(str.split(",")[2]).getTime();
        String name = str.split(",")[1];
        int index = Integer.valueOf(str.split(",")[0]);
        return new Data(index, name, date);
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
    env.setParallelism(5);
    DataStream<String> text = env.socketTextStream("localhost", 9000);
    final OutputTag<Data> outputTag = new OutputTag<>("side-output",
        TypeInformation.of(Data.class));
    SingleOutputStreamOperator<Data> dataDtream =
        text.map(x -> Data.str2Data(x))
            .assignTimestampsAndWatermarks(new MyWaterMark())
            .keyBy(x -> x.index)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .allowedLateness(Time.seconds(2))
            .sideOutputLateData(outputTag)
            .reduce((a, b) -> new Data(a.index, a.name + " " + b.name, b.date,
                a.num + b.num));
    dataDtream.print("normal print");
    dataDtream.getSideOutput(outputTag).print("side-print");
    env.execute();
  }

  private static class MyWaterMark implements AssignerWithPeriodicWatermarks<Data> {

    long maxOutOfOrder = 3000L;
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
          dateFormat.format(element.date),
          dateFormat.format(getCurrentWatermark().getTimestamp())));
      return element.date;
    }
  }
}
