package studyzk.taskAssign;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.jetty.util.thread.ExecutorThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Worker implements Watcher, Closeable {
  public static List<List<String>> processedTasks = new LinkedList<>();
  private final static Logger LOG = LoggerFactory.getLogger(Worker.class);

  private String name;
  private final String connectString;
  private ZooKeeper zooKeeper;
  private CountDownLatch latch = new CountDownLatch(1);
  private ChildrenCache ownedTasks = new ChildrenCache();
  private ExecutorThreadPool executors = new ExecutorThreadPool(1, 1, 600,
      TimeUnit.SECONDS, new ArrayBlockingQueue<>(200));

  public Worker(String name, String connectString) {
    this.name = name;
    this.connectString = connectString;
  }

  private void init() throws InterruptedException, IOException, KeeperException {
    zooKeeper = new ZooKeeper(connectString, 15000, this);
    latch.await();
    Common.Prepare.prepare(zooKeeper);
  }

  private void register() throws KeeperException, InterruptedException {
    try {
      zooKeeper.create(Common.workerRegisterPath + "/" + name, new byte[0],
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (NodeExistsException e) {
      LOG.info("Path {} exists when create this path.", e.getPath());
    }
    monitorAssignNode();
  }

  private void monitorAssignNode() throws KeeperException, InterruptedException {
    Stat stat = zooKeeper.exists(Common.workerAssignPath + "/" + name,
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (event.getType() == EventType.NodeCreated) {
              try {
                monitorTasks();
              } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to monitor tasks");
              }
            }
          }
        });
    if (stat != null) {
      monitorTasks();
    }
  }

  private void monitorTasks() throws KeeperException, InterruptedException {
    LOG.info("Worker will monitor tasks");
    List<String> tasks =
        zooKeeper.getChildren(Common.workerAssignPath + "/" + name,
        new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == EventType.NodeChildrenChanged) {
          try {
            monitorTasks();
          } catch (KeeperException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });
    List<String> needProcessTask = ownedTasks.getAddNode(tasks);
    ownedTasks.updateList(tasks);
    processTasks(Common.workerAssignPath + "/" + name, needProcessTask);
  }

  private void processTasks(String root, List<String> tasks) {

    if (tasks == null) {
      return;
    }
    LOG.info("Will process new {} tasks.", tasks.size());
    processedTasks.add(new LinkedList<>(tasks));
    tasks.forEach(x -> executors.execute(new Runnable() {
      private String path;
      @Override
      public void run() {
        try {
          String data = new String(zooKeeper.getData(path, false, null));
          // currently we get the data, and could deal with the task.
          Thread.sleep(500);
          String taskname = path.substring(path.lastIndexOf('/') + 1);
          // after process the task, then create finish node.
          try {
            zooKeeper.create(Common.tasksStatusPath + "/" + taskname,
                "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          } catch (ConnectionLossException e) {
            // currently will only do one retry, need think about zk
            // connection lost issue.
            LOG.warn("First time failed to create " + taskname);
            zooKeeper.create(Common.tasksStatusPath + "/" + taskname,
                "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          }

          // delete task assign path.
          zooKeeper.delete(path, -1);
          LOG.info("Task {} has been processed.", taskname);
        } catch (KeeperException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      public Runnable init(String path) {
        this.path = path;
        return this;
      }
    }.init(root + "/" + x)));
  }


  public void start() throws InterruptedException, IOException, KeeperException {
    init();
    register();
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
      if (event.getState() == KeeperState.SyncConnected) {
        latch.countDown();
      } else if (event.getState() == KeeperState.Expired) {
        throw new RuntimeException("Session expired");
      }
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
    Worker worker = new Worker("work1", "localhost:2181");
    worker.init();
    worker.register();
    worker.start();
  }
}
