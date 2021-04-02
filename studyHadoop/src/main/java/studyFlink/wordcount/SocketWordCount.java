package studyFlink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 *
 */
public class SocketWordCount {
  public static void main(String[] args) throws Exception {
//    if (args.length < 2) {
//      System.out.println("usage: hostname port");
//      return;
//    }
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//    env.socketTextStream(args[0], Integer.valueOf(args[1]).intValue())
//        .flatMap(new LineSplit())
//        .keyBy(x -> x.f0)
//        .timeWindow(Time.seconds(3L))
//        .sum(1)
//        .print();
//    env.execute();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    env.
        fromElements(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L)
        .keyBy(new KeySelector<Long, Long>() {
          public Long getKey(Long key) throws Exception {
            return key % 2;
          }
        }).timeWindow(Time.seconds(5))//满3个元素时触发计算
        .reduce(new ReduceFunction<Long>() {
          @Override
          public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
          }
        }).setParallelism(1)
        .addSink(new RichSinkFunction<Long>() {
          @Override
          public void invoke(Long value, Context context) throws Exception {
            System.out.println("hello world: " + value);
          }
        });
    env.execute();
  }
}

class LineSplit implements FlatMapFunction<String, Tuple2<String, Long>> {

  @Override
  public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
    String[] values = value.split("\\W+");
    for (String item : values) {
      out.collect(new Tuple2<>(item, 1L));
    }
  }
}
