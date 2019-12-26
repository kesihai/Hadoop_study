package studyzk.taskAssign;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class BaseTest {
  private static final Logger LOG = LoggerFactory.getLogger(studyzk.taskAssign.BaseTest.class);

  private File file;
  private String tempDir = "./temp";
  StandaloneServer zkServer;

  @Before
  public void setUp() throws Exception {
    File preDir = new File(tempDir);
    if (!preDir.exists() && !preDir.mkdir()) {
      throw new Exception("failed to mkdir" + tempDir);
    }
    file = new File(tempDir, "zoo.cfg");
    FileWriter writer = new FileWriter(file);
    writer.write("tickTime=2000\n");
    writer.write("initLimit=10\n");
    writer.write("syncLimit=5\n");

    File dataDir = new File(tempDir, "data");
    if (!dataDir.exists() && !dataDir.mkdir()) {
      throw new IOException("unable to mkdir " + dataDir);
    }
    String dir = dataDir.toString();
    String osName = java.lang.System.getProperty("os.name");
    if (osName.toLowerCase().contains("windows")) {
      dir = dir.replace('\\', '/');
    }
    writer.write("dataDir=" + dir + "\n");
    writer.write("clientPort=" + 2181 + "\n");
    writer.flush();
    writer.close();
    zkServer = new StandaloneServer(file.toString());
    zkServer.start();
  }

  @After
  public void tearDown() throws Exception {
    this.zkServer.shutdown();
    Thread.sleep(1000);
    delete(new File(tempDir));
  }

  private void delete(File path) {
    LOG.info("will delete path {}", path);
    if (!path.exists()) {
      return;
    }
    if (path.isDirectory()) {
      Arrays.stream(path.list()).forEach(x -> delete(new File(path, x)));
    }
    path.delete();
  }
}