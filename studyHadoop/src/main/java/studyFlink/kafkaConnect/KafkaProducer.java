package studyFlink.kafkaConnect;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionPlanUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import studyFlink.source.StringSource;

public class KafkaProducer {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.addSource(new StringSource())
        .keyBy(x -> x.substring(0, 2))
        .timeWindow(Time.seconds(5L))
        .reduce(new ReduceFunction<String>() {
          @Override
          public String reduce(String s, String t1) throws Exception {
            return String.valueOf(s.length() + t1.length());
          }
        })
//        .print();
        .addSink(new FlinkKafkaProducer011<String>(
            "localhost:9092",
            "event",
            new SimpleStringSchema()
        )).name("flink-connectors-kafka");
    env.execute("Flink kafka connector");
  }
}
