package studyFlink.watermark;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopN {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    URL fileUrl = TopN.class.getClassLoader().getResource("UserBehavior.csv");
    Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
    PojoTypeInfo pojoType = (PojoTypeInfo) TypeExtractor
        .createTypeInfo(UserBehavior.class);
    String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
    PojoCsvInputFormat csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);
    DataStream<UserBehavior> dataStream = env.createInput(csvInput, pojoType);
    dataStream
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
          @Override
          public long extractAscendingTimestamp(UserBehavior element) {
            return element.timestamp * 1000;
          }
        })
        .filter(x -> x.behavior.equals("pv"))
        .keyBy("itemId")
        .timeWindow(Time.minutes(60), Time.minutes(5))
        .aggregate(new CountAgg(), new WindowResultFunction())
        .keyBy(x -> x.windowEnd)
        .process(new TopNHotItems(3))
        .print();
    env.execute("Hot Items Job");

  }

  public static class UserBehavior {
    public long userId;         // 用户ID
    public long itemId;         // 商品ID
    public int categoryId;      // 商品类目ID
    public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
    public long timestamp;      // 行为发生的时间戳，单位秒
  }

  public static class CountAgg implements
      AggregateFunction<UserBehavior, Long, Long> {

    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long acc) {
      return acc + 1;
    }

    @Override
    public Long getResult(Long acc) {
      return acc;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {
      return acc1 + acc2;
    }
  }

  public static class WindowResultFunction implements WindowFunction<Long,
      ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(
        Tuple key,  // 窗口的主键，即 itemId
        TimeWindow window,  // 窗口
        Iterable aggregateResult, // 聚合函数的结果，即 count 值
        Collector collector  // 输出类型为 ItemViewCount
    ) throws Exception {
      Long itemId = ((Tuple1<Long>) key).f0;
      Long count = (Long) aggregateResult.iterator().next();
      collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
    }
  }

  public static class ItemViewCount {
    public long itemId;
    public long windowEnd;
    public long viewCount;

    public ItemViewCount() {
    }

    public ItemViewCount(long itemId, long windowEnd, long viewCount) {
      this.itemId = itemId;
      this.windowEnd = windowEnd;
      this.viewCount = viewCount;
    }

    public static ItemViewCount of(long itemId, long windowEnd,
        long viewCount) {
      return new ItemViewCount(itemId, windowEnd, viewCount);
    }
  }

  public static class TopNHotItems extends KeyedProcessFunction<Long, 
      ItemViewCount,
        String> {
    private final int topSize;
    public TopNHotItems(int topSize) {
      this.topSize = topSize;
    }
    //用于存储商品与点击数的状态，收齐同一窗口的数据，再触发TopN计算
    private ListState<ItemViewCount> itemState;

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
      //把每条数据添加到状态中
      itemState.add(value);
      //注册windowend + 1的 eventTime
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
      List<ItemViewCount> allItems = new ArrayList<>();

      for (ItemViewCount item : itemState.get()){
        allItems.add(item);
      }
      //清除状态中的数据，释放空间
      itemState.clear();
      //定义排序规则（降序）
      allItems.sort(new Comparator<ItemViewCount>() {
        @Override
        public int compare(ItemViewCount o1, ItemViewCount o2) {
          return (int)(o2.viewCount - o1.viewCount);
        }
      });
      //将需要打印的信息转换成string，方便打印
      StringBuilder result = new StringBuilder();
      result.append("================================\n");
      result.append("时间：").append(new Timestamp(timestamp-1)).append("\n");
      for (int i=0;i<allItems.size() && i< topSize;i++){
        ItemViewCount currentItem = allItems.get(i);
        //No1:  商品ID=1111   浏览量=11
        result.append("No")
            .append(i)
            .append(":")
            .append("   商品ID=")
            .append(currentItem.itemId)
            .append("   浏览量=")
            .append(currentItem.viewCount)
            .append("\n");
      }
      result.append("================================\n\n");

      //控制输出频率
      Thread.sleep(1000);
      out.collect(result.toString());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      ListStateDescriptor<ItemViewCount> itemStateDesc = new ListStateDescriptor<>("itemState-state", ItemViewCount.class);
      itemState = getRuntimeContext().getListState(itemStateDesc);
    }
  }



}
