package studyzk.taskAssign;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for some common parameters/config.
 */
public interface Common {

  String masterPath = "/master";

  String workerAssignPath = "/assign";
  String workerRegisterPath = "/workers";

  String tasksCreatePath =  "/create";
  String tasksStatusPath =  "/status";

  final class Prepare {
    private final static Logger LOG = LoggerFactory.getLogger(Prepare.class);

    public static void prepare(ZooKeeper zk) throws KeeperException,
        InterruptedException {
      createNode(zk, workerAssignPath);
      createNode(zk, workerRegisterPath);
      createNode(zk, tasksCreatePath);
      createNode(zk, tasksStatusPath);
    }

    private static void createNode(ZooKeeper zk, String path)
        throws KeeperException, InterruptedException {
      try {
        zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (NodeExistsException e) {
        LOG.info("node {} already exists when create it, then ignore it.", path);
      }
    }
  }
}
