package studyzk.taskAssign;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ClientTest extends BaseTest {

  @Test(timeout = 10000)
  public void submitTask() throws IOException, InterruptedException, KeeperException {
    Client client = new Client("localhost:2181");
    client.startZk();
    while (!client.isConnected()) {
      Thread.sleep(500);
    }
    client.init();
    ZkTask task = new ZkTask("task1");
    client.submitTask(task);
    task.waitUntilDone();
    Assert.assertTrue("Task not done", task.done);
  }
}