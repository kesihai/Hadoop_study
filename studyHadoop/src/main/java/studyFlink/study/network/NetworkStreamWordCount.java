package studyFlink.study.network;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Date;

public class NetworkStreamWordCount {
  private final static Logger LOG =
      LoggerFactory.getLogger(NetworkStreamWordCount.class);

  public static void main(String[] args) throws Exception {
    LOG.info("hello world");
    if (args.length < 2) {
      args = new String[]{"localhost", "9000"};
    }
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);

    env.addSource(new Source())
        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Item>() {
          long curTimeStamp = 0;
          @Nullable
          @Override
          public Watermark getCurrentWatermark() {
            return new Watermark(curTimeStamp);
          }

          @Override
          public long extractTimestamp(Item element, long previousElementTimestamp) {
            curTimeStamp = Math.max(curTimeStamp, element.getTimestamp());
            return curTimeStamp;
          }
        })
        .keyBy(x -> x.getName())
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce((a, b) -> {
          a.setCount(a.count + b.count);
          return a;
        })
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce((a, b) -> new Item(a.name + "_" + b.name, a.count + b.count, a.timestamp))
        .print();
    env.execute("hello world");
  }

  @Data
  @AllArgsConstructor
  public static class Item {
    String name;
    long count;
    long timestamp;

    @Override
    public String toString() {
      return "Item{" +
          "name='" + name + '\'' +
          ", count=" + count +
          ", timestamp=" + new Date(timestamp) +
          '}';
    }
  }

  @Slf4j
  public static class Source implements SourceFunction<Item> {

    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
      long count = 0;
      while (true) {
        long time = System.currentTimeMillis();
        ctx.collect(new Item(
            time % 2 == 0 ? "a" : "b",
            1L,
            time
        ));
        log.info("" + (++count));
        Thread.sleep(100);
      }
    }

    @Override
    public void cancel() {
    }
  }
}
