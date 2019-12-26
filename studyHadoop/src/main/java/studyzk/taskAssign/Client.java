package studyzk.taskAssign;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;

/**
 * This class is to create tasks in zk, and monitor the tasks status.
 */
public class Client implements Watcher, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Client.class);
  private ZooKeeper zooKeeper;
  private String hostname;
  private boolean connected = false;
  private HashMap<String, Object> taskMap = new HashMap<>();

  public Client(String hostname) {
    this.hostname = hostname;
  }

  public void startZk() throws IOException {
    zooKeeper = new ZooKeeper(hostname, 15000, this);
  }

  public void init() throws KeeperException, InterruptedException {
    Common.Prepare.prepare(zooKeeper);
  }

  public boolean isConnected() {
    return connected;
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    if (watchedEvent.getType() == Event.EventType.None) {
      switch (watchedEvent.getState()) {
        case SyncConnected:
          connected = true;
          break;
        case Disconnected:
        case Expired:
          connected = false;
          break;
        default:
          break;
      }
    }
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

  public void submitTask(ZkTask task) {
    zooKeeper.create(
        Common.tasksCreatePath + "/task-",
        task.task.getBytes(),
        ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT_SEQUENTIAL, createTaskCallback, task);
  }

  StringCallback createTaskCallback = new StringCallback() {
    @Override
    public void processResult(int i, String path, Object o, String name) {
      switch (Code.get(i)) {
        case OK:
          LOG.info("My created task name: " + name);
          ((ZkTask) o).setName(name);
          String assignPath = Common.tasksStatusPath
              + name.substring(name.lastIndexOf('/'));
          // since we created the tasks/task-0
          // will monitor the assignTasks/task-0
          watchStatus(assignPath, o);
          break;
        case CONNECTIONLOSS:
          submitTask((ZkTask) o);
          break;
        default:
          LOG.error("something went wrong" + KeeperException.create(Code.get(i), path));
      }
    }
  };

  void watchStatus(String path, Object ctx) {
    taskMap.put(path, ctx);
    zooKeeper.exists(path, statusWatcher, statCallback, ctx);
  }

  StatCallback statCallback = new StatCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      switch (KeeperException.Code.get(rc)) {
        case CONNECTIONLOSS:
          watchStatus(path, ctx);
          break;
        case OK:
          if (stat != null) {
            zooKeeper.getData(path, false, dataCallback, ctx);
          }
          break;
        case NONODE:
          break;
        default:
          LOG.error("something went wrong when checking if the status code " +
              "exists:" + KeeperException.create(Code.get(rc), path));
          break;
      }
    }
  };

  Watcher statusWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == Event.EventType.NodeCreated) {
        assert event.getPath().contains(Common.tasksStatusPath);
        assert taskMap.containsKey(event.getPath());
        zooKeeper.getData(event.getPath(), false, dataCallback,
            taskMap.get(event.getPath()));
      }
    }
  };

  AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          zooKeeper.getData(path, false, dataCallback, ctx);
          break;
        case OK:
          String taskResult = new String(data);
          LOG.info("Task " + path + ", " + taskResult);
          assert (ctx != null);
          ((ZkTask)ctx).setStatus(taskResult.contains("done"));

          zooKeeper.delete(path, -1, deleteCallback, null);
          taskMap.remove(path);
          break;
        case NONODE:
          LOG.warn("status node is gone!");
          return;
        default:
          LOG.error("Something went wrong here, " + KeeperException.create(KeeperException.Code.get(rc), path));
          break;
      }
    }
  };

  VoidCallback deleteCallback = new VoidCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          zooKeeper.delete(path, -1, deleteCallback, null);
          break;
        case NONODE:
          LOG.warn("This path already had been deleted: " + path);
          break;
        case OK:
          LOG.warn("This path is deleted successfully :" + path);
          break;
        default:
          LOG.error("Something went wrong when delete " + path);
          break;
      }
    }
  };

  public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
    Client client = new Client("localhost:2181");
    client.startZk();
    while (!client.isConnected()) {
      Thread.sleep(100);
    }
    client.init();
    ZkTask task1 = new ZkTask("task1");
    ZkTask task2 = new ZkTask("task2");

    task1.waitUntilDone();
    task2.waitUntilDone();
  }
}
