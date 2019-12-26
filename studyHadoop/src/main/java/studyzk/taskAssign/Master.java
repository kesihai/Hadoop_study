package studyzk.taskAssign;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
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
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class Master implements Watcher, Closeable {

  enum MasterStatus {RUNNING, ELECTED, NOTELECTED}
  private static final Logger LOG = LoggerFactory.getLogger(Master.class);
  private String name;
  private String connectString;
  private ZooKeeper zooKeeper;
  private MasterStatus status = MasterStatus.RUNNING;
  private ChildrenCache taskscache = new ChildrenCache();
  private ChildrenCache workersChace = new ChildrenCache();
  private CountDownLatch latch = new CountDownLatch(1);
  private Random random = new Random();

  public Master(String name, String connectString) {
    this.name = name;
    this.connectString = connectString;
  }

  public void start() throws IOException, InterruptedException {
    zooKeeper = new ZooKeeper(connectString, Common.timeout, this);
    latch.await();
    Common.Prepare.prepare(zooKeeper);
    tryToBecomeActive();
  }

  private void tryToBecomeActive() {
    try {
      if (becomeActive()) {
        LOG.info("{} become active and will take lead ship.", name);
        takeLeadShip();
      } else {
        monitorMasterStatus();
      }
    } catch (Exception e) {
      throw new RuntimeException("Meet exception when " + name + " try to " +
          "become active master: " + e) ;
    }
  }

  private boolean becomeActive() throws KeeperException, InterruptedException {
    try {
      zooKeeper.create(Common.masterPath, name.getBytes(),
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      return true;
    } catch (NodeExistsException e) {
    }
    return false;
  }

  private void monitorMasterStatus() throws KeeperException, InterruptedException {
    zooKeeper.exists(Common.masterPath, monitorMasterWatcher);
  }

  private Watcher monitorMasterWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == EventType.NodeDeleted) {
          tryToBecomeActive();
      }
    }
  };

  private void takeLeadShip() throws KeeperException, InterruptedException {
    // when master start, some workers may already failed that means workers
    // exist in /workers but not exists in /assign, so let's add workers
    // under /assign to active workers and trigger workListChanged event.
    this.workersChace.updateList(getAssignWorkers());
    workerListChange();
    taskListChanged();
  }

  private List<String> getActiveWorkers() throws KeeperException, InterruptedException {
    return zooKeeper.getChildren(Common.workerRegisterPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        workerListChange();
      }
    });
  }

  private List<String> getAssignWorkers() throws KeeperException, InterruptedException {
    return zooKeeper.getChildren(Common.workerAssignPath, false);
  }

  private List<String> getTasks() throws KeeperException, InterruptedException {
    return zooKeeper.getChildren(Common.tasksCreatePath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == EventType.NodeChildrenChanged) {
          taskListChanged();
        }
      }
    });
  }

  private void workerListChange() {
    LOG.info("Worker list change.");
    try {
      List<String> activeWorkers = getActiveWorkers();
      List<String> lostWorkers = workersChace.getLostNode(activeWorkers);
      workersChace.updateList(activeWorkers);
      handleWorkLost(Common.workerAssignPath, lostWorkers);
    } catch (InterruptedException e) {
      workerListChange();
    } catch (KeeperException e) {
      throw new RuntimeException("Exception when handle workerListChange." + e);
    }
  }

  private void taskListChanged() {
    LOG.info("Task list changed.");
    try {
      List<String> tasks = getTasks();
      List<String> newTasks = taskscache.getAddNode(tasks);
      taskscache.updateList(tasks);
      doAssignment(newTasks);
    } catch (InterruptedException e) {
      taskListChanged();
    } catch (KeeperException e) {
      throw new RuntimeException("Exception when handle taskListChanged " + e);
    }
  }

  private void handleWorkLost(String path, List<String> workers) throws KeeperException, InterruptedException {
    for (String worker : workers) {
      List<String> tasks = zooKeeper.getChildren(path + '/' + worker, false);
      for (String task : tasks) {
        String taskPath = path + '/' + worker + '/' + task;
        byte[] data = zooKeeper.getData(taskPath, false, null);
        zooKeeper.create(Common.tasksCreatePath + '/' + task, data,
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.delete(taskPath, -1);
      }
      zooKeeper.delete(path + '/' + worker, -1);
    }
  }

  private void doAssignment(List<String> tasks) throws KeeperException, InterruptedException {
    if (workersChace.children.size() == 0) {
      LOG.warn("There is no active workers, won't do assinment.");
      return;
    }
    LOG.info("Will assign {} tasks.", tasks.size());
    for (String task : tasks) {
      int index = random.nextInt(workersChace.children.size());
      String worker = workersChace.children.get(index);
      assignTask(task, worker);
    }
  }

  private void assignTask(String task, String worker) throws KeeperException, InterruptedException {
    LOG.info("Will assign task {} to worker {}", task, worker);
    String path = Common.workerAssignPath + '/' + worker;
    if (zooKeeper.exists(path, false) == null) {
      zooKeeper.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    }
    String taskPath = Common.tasksCreatePath + '/' + task;
    byte[] data = zooKeeper.getData(taskPath, false, null);
    zooKeeper.create(path + '/' + task, data, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    zooKeeper.delete(taskPath, -1);
    LOG.info("Finish assign task {} to worker {}", task, worker);
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
}
