package studyFlink.broadcast;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import studyFlink.FlinkUtil;

import java.util.Arrays;

/**
 * 学习 flink broadcast:
 * 1. 一个流消费socket 得到 DataStream<String>
 * 2. 另一个流是规则流, 规则：<String, String> 将key 替换为 value, 如果没有规则，那么key保持不变.
 */
@Slf4j
public class BroadSocketExample {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<String> data = FlinkUtil.getSocketStream(env, FlinkUtil.LOCALHOST, 7777, " ");

    MapStateDescriptor<String, String> rule = new MapStateDescriptor<String, String>(
        "replaceRuleBroadcastState",
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO);

    DataStream<String> ruleStream = FlinkUtil.getSocketStream(env, FlinkUtil.LOCALHOST, 7778, " ");

    BroadcastStream<String> ruleBroad = ruleStream.broadcast(rule);

    data.connect(ruleBroad)
        .process(new BroadcastProcessFunction<String, String, String>() {
          @Override
          public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(rule);
            if (state != null && state.contains(value)) {
              value = state.get(value);
            }
            out.collect(value);
          }

          @Override
          public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
            if (!value.contains(",")) {
              return;
            }
            String[] arr = value.split(",");
            log.info("debug: {}, {}", value, Arrays.toString(arr));
            ctx.getBroadcastState(rule).put(arr[0], arr[1]);
          }
        })
        .print();

    env.execute("broadcast start");

  }

}
