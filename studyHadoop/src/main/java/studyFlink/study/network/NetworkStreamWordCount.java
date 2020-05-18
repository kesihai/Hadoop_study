package studyFlink.study.network;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import studyFlink.map.LineSplitMap;

import java.util.Arrays;

public class NetworkStreamWordCount {
  private final static Logger LOG =
      LoggerFactory.getLogger(NetworkStreamWordCount.class);

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      args = new String[]{"localhost", "9000"};
    }
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.socketTextStream(args[0], Integer.parseInt(args[1]))
        .flatMap(new LineSplitMap())
        .keyBy(0)
        .sum(1)
        .print();
    env.execute();
  }
}
