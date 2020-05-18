package studyFlink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RandomIntegerSource implements SourceFunction<Integer> {

  @Override
  public void run(SourceContext<Integer> ctx) throws Exception {
//    ctx.collectWithTimestamp();
  }

  @Override
  public void cancel() {

  }
}
