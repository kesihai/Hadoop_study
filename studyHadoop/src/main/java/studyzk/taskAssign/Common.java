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

  // Other config for zookeeper
  int timeout = 15000;
  String connectString = "localhost:2181";

  final class Prepare {
    private final static Logger LOG = LoggerFactory.getLogger(Prepare.class);

    public static void prepare(ZooKeeper zk) {
      createNode(zk, workerAssignPath);
      createNode(zk, workerRegisterPath);
      createNode(zk, tasksCreatePath);
      createNode(zk, tasksStatusPath);
    }

    private static void createNode(ZooKeeper zk, String path) {
      try {
        zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (NodeExistsException e) {
        LOG.info("Node {} already exists when create it, then ignore it.", path);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("Prepare failed to prepare.");
      }
    }
  }
}
