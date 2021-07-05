package studyFlink.watermark;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class StudyTopN {
  public static void main(String[] args) throws URISyntaxException {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    URL fileUrl = TopN.class.getClassLoader().getResource("UserBehavior.csv");
    Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
    PojoTypeInfo pojoType = (PojoTypeInfo) TypeExtractor
        .createTypeInfo(TopN.UserBehavior.class);
    String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
    PojoCsvInputFormat csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);
    DataStream<TopN.UserBehavior> dataStream = env.createInput(csvInput, pojoType);

    dataStream
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TopN.UserBehavior>() {
          @Override
          public long extractAscendingTimestamp(TopN.UserBehavior element) {
            return element.timestamp * 1000;
          }
        })
        .filter(x -> x.behavior.equals("pv"))
        .keyBy(x -> x.itemId)
        .timeWindow(Time.minutes(60), Time.minutes(5))
        .aggregate(new AggregateFunction<TopN.UserBehavior, Long, Long>() {
          @Override
          public Long createAccumulator() {
            return 0L;
          }

          @Override
          public Long add(TopN.UserBehavior value, Long accumulator) {
            return accumulator + 1L;
          }

          @Override
          public Long getResult(Long accumulator) {
            return accumulator;
          }

          @Override
          public Long merge(Long a, Long b) {
            return a + b;
          }
        }, new WindowFunction<Long, TopN.ItemViewCount, Long, TimeWindow>() {
          @Override
          public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<TopN.ItemViewCount> out) throws Exception {
            TopN.ItemViewCount count = new TopN.ItemViewCount(aLong, window.getEnd(), input.iterator().next());
            out.collect(count);
          }
        })
        .keyBy(x -> x.windowEnd)
        .process(new KeyedProcessFunction<Long, TopN.ItemViewCount, String>() {

          @Override
          public void processElement(TopN.ItemViewCount value, Context ctx, Collector<String> out) throws Exception {

          }
        })

  }

  public static class TopNProcess extends KeyedProcessFunction<Long, TopN.ItemViewCount, String> {

    ListState<TopN.ItemViewCount> list;
    @Override
    public void processElement(TopN.ItemViewCount value, Context ctx, Collector<String> out) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      list = getRuntimeContext().getListState(new ListStateDescriptor<>("item-list", TopN.ItemViewCount.class));
    }
  }




  public static class UserBehavior {
    public long userId;         // 用户ID
    public long itemId;         // 商品ID
    public int categoryId;      // 商品类目ID
    public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
    public long timestamp;      // 行为发生的时间戳，单位秒
  }
}
