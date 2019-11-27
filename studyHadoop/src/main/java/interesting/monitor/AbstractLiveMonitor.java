package interesting.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A abstract live monitor service.
 */
public abstract class AbstractLiveMonitor<O> {

  private final Log log = LogFactory.getLog(AbstractLiveMonitor.class);
  private long expireInternal;
  private long monitorInternal;
  private Map<O, Long> running = new HashMap<>();
  private volatile boolean stoped = false;
  private Thread thread = null;

  public AbstractLiveMonitor(long expireInternal, long monitorInternal) {
    this.expireInternal = expireInternal;
    this.monitorInternal = monitorInternal;
  }

  public synchronized void register(O obj) {
    register(obj, System.currentTimeMillis());
  }

  public synchronized void register(O obj, long pingTime) {
    running.put(obj, pingTime);
  }

  public synchronized void receivePing(O obj) {
    if (running.containsKey(obj)) {
      running.put(obj, System.currentTimeMillis());
    }
  }

  public synchronized void unregister(O obj) {
    running.remove(obj);
  }

  public abstract void expire(O obj);

  public void start() {
    assert !stoped : "starting when already started";
    thread = new Thread(new CheckThread(), "Check thread");
    log.info("monitor service is start");
    thread.start();
  }

  public void stop() {
    stoped = true;
    if (thread != null) {
      thread.interrupt();
    }
  }

  private class CheckThread implements Runnable {

    @Override
    public void run() {
      while (!stoped && !Thread.currentThread().isInterrupted()) {
        synchronized (AbstractLiveMonitor.this) {
          Iterator<Map.Entry<O, Long>> ite = running.entrySet().iterator();
          long currentTime = System.currentTimeMillis();
          while (ite.hasNext()) {
            Map.Entry<O, Long> entry = ite.next();
            O key = entry.getKey();
            if (currentTime > expireInternal + entry.getValue()) {
              ite.remove();
              expire(key);
            }
          }
        }
        try {
          Thread.sleep(monitorInternal);
        } catch (InterruptedException e) {
          log.info("check thread is interrupted");
          break;
        }
      }
    }
  }
}
