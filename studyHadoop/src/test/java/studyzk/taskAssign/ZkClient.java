package studyzk.taskAssign;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZkClient implements Watcher {

  public ZooKeeper zooKeeper;
  private String connectString;
  CountDownLatch latch = new CountDownLatch(1);

  public ZkClient(String connectString) {
    this.connectString = connectString;
  }

  public void startZK() throws IOException, InterruptedException {
    zooKeeper = new ZooKeeper(connectString, Common.timeout, this);
    latch.await();
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == EventType.None) {
      if (event.getState() == KeeperState.SyncConnected) {
        latch.countDown();
      }
    }
  }
}
