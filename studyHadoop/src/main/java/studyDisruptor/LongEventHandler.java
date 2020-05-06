package studyDisruptor;

import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongEventHandler implements EventHandler<LongEvent> {
  private final static Logger LOG =
      LoggerFactory.getLogger(LongEventHandler.class);

  @Override
  public void onEvent(LongEvent event, long sequence, boolean endOfBatch)
      throws Exception {
    LOG.info("event: {}, sequence: {}, endOfBatch:{}",
        event, sequence, endOfBatch);
  }
}
