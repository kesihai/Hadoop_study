package studyzk.taskAssign;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class WorkerTest extends BaseTest {

  @Test(timeout = 10000)
  public void testMonitorTasks() throws KeeperException, InterruptedException, IOException {
    String name = "work1";
    Worker worker = new Worker(name, Common.connectString);
    try {
      worker.start();
    } catch (IOException e) {
      e.printStackTrace();
    }

    ZkClient client = new ZkClient(Common.connectString);
    client.startZK();
    String taskName = "tasks-00000";
    Common.Prepare.prepare(client.zooKeeper);
    try {
      client.zooKeeper.create(Common.workerAssignPath + "/" + name,
          new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (NodeExistsException e) {
    }
      client.zooKeeper.create(
          Common.workerAssignPath + "/" + name + "/" + taskName,
          new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    Thread.sleep(5000);
    Assert.assertNotNull(client.zooKeeper.exists(Common.tasksStatusPath + "/" + taskName, false));
  }
}