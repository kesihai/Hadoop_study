package studyzk.taskAssign;

import org.apache.zookeeper.ZooKeeperMain;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Start a zookeeper standalone server.
 */
public class StandaloneServer extends Thread {
  private TestZKSMain zkMain;
  private String confFile;

  private static final Logger LOG =
      LoggerFactory.getLogger(StandaloneServer.class);

  StandaloneServer(String confFile) {
    this.confFile = confFile;
    this.zkMain = new TestZKSMain();
  }

  private static class TestZKSMain extends ZooKeeperServerMain {
    @Override
    public void initializeAndRun(String[] args) throws ConfigException,
        IOException {
      super.initializeAndRun(args);
    }
    public void shutdown() {
      super.shutdown();
    }
  }

  @Override
  public void run() {
    try {
      zkMain.initializeAndRun(new String[]{confFile});
    } catch (ConfigException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void shutdown() {
    zkMain.shutdown();
  }
}
