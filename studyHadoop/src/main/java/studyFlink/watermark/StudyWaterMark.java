package studyFlink.watermark;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import studyFlink.map.LineSplitMap;

import javax.annotation.Nullable;

public class StudyWaterMark {
  public static void main(String[] args) {
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.socketTextStream(args[0], Integer.parseInt(args[1]))
        .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() {
          @Nullable
          @Override
          public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
            return null;
          }

          @Override
          public long extractTimestamp(String element, long previousElementTimestamp) {
            return 0;
          }
        });
  }
}
