package studyzk.taskAssign;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Worker implements Watcher, Closeable {
  private final static Logger LOG = LoggerFactory.getLogger(Worker.class);

  private String connectString;
  private String workerName;
  private String registerPath;
  private String assignPath;
  private ZooKeeper zooKeeper;
  private ThreadPoolExecutor executor;
  private boolean connected = false;
  private boolean expired = false;
  private ChildrenCache childrenCache = new ChildrenCache();

  public Worker(String connectString, String workerName) {
    this.connectString = connectString;
    this.workerName = workerName;
    this.registerPath = Common.workerRegisterPath + "/" + workerName;
    this.assignPath = Common.workerAssignPath + "/" + workerName;
  }

  public void init() throws IOException {
    zooKeeper = new ZooKeeper(connectString, 15000, this);
    executor = new ThreadPoolExecutor(1, 1, 1000, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(200));
  }

  /**
   * bootstrap is to create /assign parent node.
   */
  public void bootstrap() {
    createAssignNode();
  }

  private void createAssignNode() {
    zooKeeper.create(assignPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT, createCallback, null);
  }

  StringCallback createCallback = new StringCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          createAssignNode();
          break;
        case NODEEXISTS:
          LOG.warn("This path already exists: " + name);
          break;
        case OK:
          LOG.info("create path successfully: " + name);
          break;
        default:
          LOG.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
          break;
      }
    }
  };

  /**
   * Registering the new worker by adding a worker znode to /workers.
   */
  public void register() {
    zooKeeper.create(
        registerPath,
        new byte[0],
        Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL,
        registerCallback,
        null);
  }

  StringCallback registerCallback = new StringCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          register();
          break;
        case NODEEXISTS:
          LOG.warn("THis path already exists: " + name);
          break;
        case OK:
          LOG.info("create path successfully: " + name);
          break;
        default:
          LOG.error("Something went wrong " + KeeperException.create(Code.get(rc), path));
          break;
      }
    }
  };

  public void getTasks() {
    zooKeeper.getChildren(
        assignPath,
        taskMonitorWatcher,
        childrenCallback,
        null);
  }

  Watcher taskMonitorWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == EventType.NodeChildrenChanged) {
        getTasks();
      }
    }
  };

  ChildrenCallback childrenCallback = new ChildrenCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          getTasks();
          break;
        case OK:
          executor.execute(new Runnable() {
            List<String> children;
            DataCallback cb;

            public Runnable init(List<String> children, DataCallback cb) {
              this.children = children;
              this.cb = cb;
              return this;
            }

            @Override
            public void run() {
              if (children == null) {
                return;
              }
              LOG.info("looping into tasks");
              setStatus("working");
              for (String task : children) {
                LOG.info("new task: {}", task);
                zooKeeper.getData(assignPath + "/" + task, false, cb, task);
              }
            }
          }.init(childrenCache.addedAndSet(children), dataCallback));
          break;
        default:
          LOG.error("getChildren failed: " + KeeperException.create(Code.get(rc), path));
          break;
      }
    }
  };

  DataCallback dataCallback = new DataCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          zooKeeper.getData(path, false, dataCallback, path);
          break;
        case OK:
          executor.execute(new Runnable() {
            byte[] data;
            Object ctx;

            public Runnable init(byte[] data, Object ctx) {
              this.data = data;
              this.ctx = ctx;
              return this;
            }

            @Override
            public void run() {
              LOG.info("Currently we got task {} data", new String(data));
              LOG.info("will sleep 2 seconds...");
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
              }
              zooKeeper.create(Common.tasksStatusPath + "/" + ctx,
                  "done".getBytes(), Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT, taskStatusCreateCallback, null);
              zooKeeper.delete(assignPath + "/" + ctx, -1, deleteTaskCallback
                  , null);
            }
          }.init(data, ctx));
      }
    }
  };

  StringCallback taskStatusCreateCallback = new StringCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          zooKeeper.create(path, "done".getBytes(), Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT, taskStatusCreateCallback, null);
          break;
        case NODEEXISTS:
          LOG.warn("Node exists: " + path);
          break;
        case OK:
          LOG.info("Create status znode correctly: " + name);
          break;
        default:
          LOG.error("Failed to create task status: " + KeeperException.create(Code.get(rc), path));
      }
    }
  };

  VoidCallback deleteTaskCallback = new VoidCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
        case OK:
        default:
          LOG.error("Failed to delete task data: " + KeeperException.create(Code.get(rc), path));
      }
    }
  };

  private void setStatus(String status) {

  }

  @Override
  public void close() throws IOException {
    try {
      zooKeeper.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == EventType.None) {
      switch (event.getState()) {
        case SyncConnected:
          connected = true;
          expired = false;
          break;
        case Disconnected:
          LOG.warn("Disconnected from zk !");
          connected = false;
        case Expired:
          connected = false;
          expired = true;
          break;
        default:
          break;
      }
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Worker worker = new Worker("localhost:2181", "work1");
    worker.init();
    while (!worker.connected) {
      LOG.info("waiting for zk connection");
      Thread.sleep(100);
    }

    worker.bootstrap();
    worker.register();
    worker.getTasks();

    while (!worker.expired) {
      Thread.sleep(1000);
    }
  }
}
