package studyzk.taskAssign;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * This class is to create tasks in zk, and monitor the tasks status.
 */
public class Client implements Watcher, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Client.class);

  private final String connectString;
  private ZooKeeper zooKeeper;
  private CountDownLatch latch = new CountDownLatch(1);
  private ConcurrentHashMap<String, ZkTask> submittedTasks = new ConcurrentHashMap<>();

  public Client(String connectString) {
    this.connectString = connectString;
  }

  public void init() throws IOException, InterruptedException, KeeperException {
    zooKeeper = new ZooKeeper(connectString, 15000, this);
    latch.await();
    Common.Prepare.prepare(zooKeeper);
  }

  public void submitTask(ZkTask task) throws KeeperException, InterruptedException {
    String path = zooKeeper.create(Common.tasksCreatePath + "/task-",
        task.task.getBytes(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT_SEQUENTIAL);
    String taskName = path.substring(path.lastIndexOf('/') + 1);
    task.setName(taskName);
    submittedTasks.put(task.name, task);
    // TO DO:
    // Currently we only monitor this task, for some case if this task finished.
    // and the client failed, then this finished task will not be found by
    // client.
    monitorTask(Common.tasksStatusPath + "/" + taskName);
  }

  public void monitorTask(String path) throws KeeperException, InterruptedException {
    zooKeeper.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == EventType.NodeCreated) {
          try {
            taskFinished(path);
          } catch (KeeperException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });
  }

  private void taskFinished(String path) throws KeeperException, InterruptedException {
    String taskName = path.substring(path.lastIndexOf('/') + 1);
    String result = new String(zooKeeper.getData(path, false, null));
    assert submittedTasks.containsKey(taskName);
    submittedTasks.get(taskName).setStatus(result.contains("done"));
    submittedTasks.remove(taskName);
    LOG.info("task {} finished with status {}.", taskName, result.contains(
        "done"));
  }

  @Override
  public void close() throws IOException {
    if (zooKeeper != null) {
      try {
        zooKeeper.close();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == EventType.None) {
      if (event.getState() == KeeperState.SyncConnected) {
        latch.countDown();
      }
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
    Client client = new Client("localhost:2181");
    client.init();
    ZkTask task1 = new ZkTask("task1");
    client.submitTask(task1);
    task1.waitUntilDone();

  }
}
