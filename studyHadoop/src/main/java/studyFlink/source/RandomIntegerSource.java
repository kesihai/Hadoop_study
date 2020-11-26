package studyFlink.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomIntegerSource implements SourceFunction<Long> {

  Random random = new Random();
  public  static volatile int count = 0;

  @Override
  public void run(SourceContext<Long> ctx) throws Exception {
    while (true) {
      synchronized (RandomIntegerSource.class) {
        ctx.collect(count % 2 * 1L);
        count += 1;
        count %= 2;
      }
      Thread.sleep(100);
    }
  }

  @Override
  public void cancel() {

  }
}
