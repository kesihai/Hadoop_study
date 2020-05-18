package studyFlink.map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class LineSplitMap extends
    RichFlatMapFunction<String, Tuple2<String, Long>> {

  @Override
  public void flatMap(String s, Collector<Tuple2<String, Long>> collector)
      throws Exception {
    Arrays.asList(s.split(" ")).forEach(item ->
        collector.collect(new Tuple2<>(item, 1L)));
  }
}
