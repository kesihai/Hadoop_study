package studyFlink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 *
 */
public class SocketWordCount {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("usage: hostname port");
      return;
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    env.socketTextStream(args[0], Integer.valueOf(args[1]).intValue())
        .flatMap(new LineSplit())
        .keyBy(x -> x.f0)
        .timeWindow(Time.seconds(3L))
        .sum(1)
        .print();
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
