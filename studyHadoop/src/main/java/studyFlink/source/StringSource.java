package studyFlink.source;

import java.util.Random;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class StringSource implements SourceFunction<String> {
  private Random random = new Random();

  @Override
  public void cancel() {
  }

  @Override
  public void run(SourceContext<String> ctx) throws Exception {
    while (true) {
      String outline = String.format(
          "{\"user_id\": \"%s\", \"item_id\":\"%s\", \"category_id\": \"%s\", \"behavior\": \"%s\"}",
          random.nextInt(10),
          random.nextInt(100),
          random.nextInt(1000),
          "pv");
      ctx.collect(outline);
      Thread.sleep(200);
    }
  }
}
