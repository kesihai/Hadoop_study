package interesting.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * AbstractLiveMonitorTest.
 */
public class AbstractLiveMonitorTest {

  private static final List<Integer> LIST = new LinkedList<>();
  private static final Log LOG =
      LogFactory.getLog(AbstractLiveMonitorTest.class);
  private static final long EXPIRE_INTERNAL = 1000 * 5;
  private static final long MONITOR_INTERNAL = 100;

  private static AbstractLiveMonitor<Integer> getMonitorImpl() {
    return new AbstractLiveMonitor<Integer>(EXPIRE_INTERNAL, MONITOR_INTERNAL) {

      @Override
      public void expire(Integer obj) {
        LOG.info("expire " + obj);
        LIST.add(obj);
      }
    };
  }

  @Test
  public void expire() throws InterruptedException {
    AbstractLiveMonitor<Integer> monitor = getMonitorImpl();
    monitor.start();
    monitor.register(1);
    monitor.register(2);
    monitor.register(3);
    Thread.sleep(EXPIRE_INTERNAL / 2);
    monitor.receivePing(1);
    monitor.receivePing(2);
    Thread.sleep(EXPIRE_INTERNAL / 2 + MONITOR_INTERNAL);
    monitor.stop();

    Assert.assertTrue(LIST.size() == 1);
    Assert.assertTrue(LIST.get(0).intValue() == 3);
  }
}