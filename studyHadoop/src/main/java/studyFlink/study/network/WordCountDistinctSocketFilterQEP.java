package studyFlink.study.network;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WordCountDistinctSocketFilterQEP {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // @formatter:off
    env.socketTextStream("localhost", 9000)
        .map(x -> 1)
        .timeWindowAll(Time.seconds(5))
        .reduce((x, y) -> x + y)
        .print();

    String executionPlan = env.getExecutionPlan();
    System.out.println(executionPlan);
//    env.execute("WordCountDistinctSocketFilterQEP");

  }

}