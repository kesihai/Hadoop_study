package studyzk.taskAssign;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
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

public class Master implements Watcher, Closeable {

  enum MasterStatus {RUNNING, ELECTED, NOTELECTED}
  private static final Logger LOG = LoggerFactory.getLogger(Master.class);
  private String name;
  private String connectString;
  private ZooKeeper zooKeeper;
  private boolean connected = false;
  private boolean expired = false;
  private MasterStatus status = MasterStatus.RUNNING;
  private ChildrenCache taskscache = new ChildrenCache();
  private ChildrenCache workersChace = new ChildrenCache();

  public Master(String hostname, String name) {
    this.connectString = hostname;
    this.name = name;
  }

  public void startZk() throws IOException {
    zooKeeper = new ZooKeeper(connectString, 15000, this);
  }

  public void bootStrap() {
    createParent(Common.workerRegisterPath, new byte[0]);
    createParent(Common.tasksCreatePath, new byte[0]);
    createParent(Common.workerAssignPath, new byte[0]);
    createParent(Common.tasksStatusPath, new byte[0]);
  }

  private void createParent(String path, byte[] data) {
    zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
        createparentCallback, data);
  }

  private StringCallback createparentCallback = new StringCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          createParent(path, (byte[])ctx);
          break;
        case NODEEXISTS:
          LOG.info("node {} already exists.", path);
          break;
        case OK:
          LOG.info(" create node {} successfully", path);
          break;
        default:
          LOG.error("something went wrong  " + KeeperException.create(Code.get(rc), path));
      }
    }
  };

  public void runForMaster() {
    LOG.info("running for master");
    zooKeeper.create(Common.masterPath, name.getBytes(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL, masterCreateCallback, null);
  }

  private StringCallback masterCreateCallback = new StringCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          checkMaster();
          break;
        case NODEEXISTS:
          status = MasterStatus.NOTELECTED;
          masterExists();
          break;
        case OK:
          takeLeadShip();
          break;
        default:
          LOG.error("something went wrong when runForMaster.");
      }
    }
  };

  private void checkMaster() {
    zooKeeper.getData(Common.masterPath, false, checkMasterCallback, null);
  }

  private DataCallback checkMasterCallback = new DataCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          checkMaster();
          break;
        case NONODE:
          runForMaster();
          break;
        case OK:
          if (name.equals(new String(data))) {
            status = MasterStatus.ELECTED;
            takeLeadShip();
          } else {
            status = MasterStatus.NOTELECTED;
            masterExists();
          }
          break;
        default:
          LOG.error("something went wrong when read data." + KeeperException.create(Code.get(rc), path));
      }
    }
  };

  private void takeLeadShip() {
    LOG.info("Is going to take a leadship");

  }

  private void getWorkers() {
    zooKeeper.getChildren(Common.workerRegisterPath, workersChangeWatcher,
        getWorkersCallback, null);
  }

  private Watcher workersChangeWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == EventType.NodeChildrenChanged) {
        assert Common.workerRegisterPath.equals(event.getPath());
        getWorkers();
      }
    }
  };

  private ChildrenCallback getWorkersCallback = new ChildrenCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          getWorkers();
          break;
        case OK:
          LOG.info("Successfully got {} workers.", children.size());
          reassignAndSet(children);
          break;
        default:
          LOG.error("getChildren failed" + KeeperException.create(Code.get(rc), path));
      }
    }
  };

  private void reassignAndSet(List<String> children) {
    List<String> toProcess = workersChace.removedAndSet(children);
    toProcess.forEach(x -> getAbsentWorkerTasks(x));

  }

  private void getAbsentWorkerTasks(String worker) {
    zooKeeper.getChildren(Common.workerAssignPath + "/" + worker, false,
        workerAssignCallback, null);
  }

  private ChildrenCallback workerAssignCallback = new ChildrenCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          getAbsentWorkerTasks(path.substring(path.lastIndexOf('/') + 1));
          break;
        case OK:
          LOG.info("successfully got a list of assignments: {} tasks",
              children.size());
          children.forEach(task -> getDataReassign(path + "/" + task, task));
          break;
        default:
          LOG.error("getChildren failed" + KeeperException.create(Code.get(rc), path));
      }
    }
  };

  private void getDataReassign(String path, String task) {
    zooKeeper.getData(path, false, getDataCallback, task);
  }

  private DataCallback getDataCallback = new DataCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      switch (Code.get(rc))  {
        case CONNECTIONLOSS:
          getDataReassign(path, (String)ctx);
          break;
        case OK:
          recreateTask(new RecreateTaskCtx(path, (String)ctx, data));
        default:
          LOG.error("Something went wrong when getting data " + KeeperException.create(Code.get(rc), (String)ctx));
      }
    }
  };

  private class RecreateTaskCtx {
    String path;
    String task;
    byte[] data;

    RecreateTaskCtx(String path, String task, byte[] data) {
      this.path = path;
      this.task = task;
      this.data = data;
    }
  }

  private void recreateTask(RecreateTaskCtx task) {
    zooKeeper.create(Common.tasksCreatePath, task.data, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT, recreateTaskcallback, task);
  }

  StringCallback recreateTaskcallback = new StringCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          recreateTask((RecreateTaskCtx)ctx);
          break;
        case OK:

      }
    }
  };

  private void deleteAssignment(String path) {

  }

  private void masterExists() {
    zooKeeper.exists(Common.masterPath, masterExistisWatcher,
        masterExistsCallback, null);
  }

  private Watcher masterExistisWatcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == EventType.NodeDeleted) {
        runForMaster();
      }
    }
  };

  private StatCallback masterExistsCallback = new StatCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          checkMaster();
          break;
        case NONODE:
          runForMaster();
          break;
        case OK:
          break;
        default:
          LOG.error("Something went wrong when monitor master node."
              + KeeperException.create(Code.get(rc), path));
      }
    }
  };

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
      switch (event.getState()) {
        case SyncConnected:
          connected = true;
          expired = false;
          break;
        case Disconnected:
          connected = false;
          break;
        case Expired:
          LOG.error("session expiration");
          connected = false;
          expired = true;

          break;
        default:
          break;
      }
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Master master = new Master("localhost:2181", args[0]);
    master.startZk();
    while (!master.connected) {
      Thread.sleep(100);
    }

    master.bootStrap();
    master.runForMaster();
    while (!master.expired) {
      Thread.sleep(1000);
    }
    master.close();
  }
}
