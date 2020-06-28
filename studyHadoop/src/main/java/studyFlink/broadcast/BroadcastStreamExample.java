package studyFlink.broadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * 测试
 */
public class BroadcastStreamExample {
  public static final Logger LOG =
      LoggerFactory.getLogger(BroadcastStreamExample.class);

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStreamSource<String> data = env.socketTextStream("localhost", 7777);
    DataStreamSource<String> stateSource =
        env.socketTextStream("localhost", 7778);
    BroadcastStream<String> broadcastStream =
        stateSource.broadcast(MultipleState.mapStateDesc);
    data.connect(broadcastStream)
        .process(new MultipleState())
        .addSink(new PrintSinkFunction<>());
    env.execute();
  }

  private static class MultipleState extends
      BroadcastProcessFunction<String, String, String> {

    private List<Integer> bufferedValues = new LinkedList<>();

    public static final MapStateDescriptor<String, Integer> mapStateDesc =
        new MapStateDescriptor<String, Integer>("multiply",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO);

    @Override
    public void processElement(
        String value,
        ReadOnlyContext ctx,
        Collector<String> out) throws Exception {
      int number = Integer.parseInt(value);
      Integer factor = ctx.getBroadcastState(mapStateDesc).get("value");
      if (factor == null) {
        bufferedValues.add(number);
        LOG.info("buffered:" + number);
      } else {
        for (Integer item : bufferedValues) {
          out.collect(Integer.toString(factor * item));
        }
        out.collect(Integer.toString(factor * number));
        bufferedValues.clear();
      }
    }

    @Override
    public void processBroadcastElement(
        String value,
        Context ctx,
        Collector<String> out) throws Exception {
      int factor = Integer.parseInt(value);
      ctx.getBroadcastState(mapStateDesc).put("value", factor);
      LOG.info("broad case state changed: " + factor);
    }
  }
}
