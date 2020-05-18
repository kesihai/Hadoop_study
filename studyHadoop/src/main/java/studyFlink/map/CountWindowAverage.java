package studyFlink.map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountWindowAverage extends
    RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

  private transient ValueState<Tuple2<Long, Long>> sum;

  @Override
  public void flatMap(
      Tuple2<Long, Long> longLongTuple2,
      Collector<Tuple2<Long, Long>> collector) throws Exception {
    Tuple2<Long, Long> current = sum.value();
    if (current == null) {
      current = new Tuple2<>(0L, 0L);
    }
    current.f0 += 1;
    current.f1 += longLongTuple2.f1;
    sum.update(current);
    if (current.f0 >= 2) {
      collector.collect(new Tuple2<>(longLongTuple2.f0,
          current.f1 / current.f0));
      sum.clear();
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
        new ValueStateDescriptor<Tuple2<Long, Long>>(
            "average",
            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));
  }
}
