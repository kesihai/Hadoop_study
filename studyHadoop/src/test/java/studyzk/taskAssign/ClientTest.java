package studyzk.taskAssign;

import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ClientTest extends BaseTest {

  @Test(timeout = 105000)
  public void submitTask() throws IOException, InterruptedException, KeeperException {

    List<Master> masters = new LinkedList<>();
    List<Worker> workers = new LinkedList<>();
    for (int i = 0; i < 5; i++) {
      masters.add(new Master("master" + i, Common.connectString));
      new Thread() {
        private Master master;

        public Thread init(Master master) {
          this.master = master;
          return this;
        }

        @Override
        public synchronized void start() {
          try {
            master.start();
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }.init(masters.get(i)).start();
    }

    for (int i = 0; i < 30; i++) {
      workers.add(new Worker("worker" + i, Common.connectString));
      new Thread() {
        private Worker worker;

        public Thread init(Worker worker) {
          this.worker = worker;
          return this;
        }

        @Override
        public synchronized void start() {
          try {
            worker.start();
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          } catch (KeeperException e) {
            e.printStackTrace();
          }
        }
      }.init(workers.get(i)).start();
    }

    Client client = new Client(Common.connectString);
    client.init();
    List<ZkTask> tasks = new LinkedList<>();
    for (int i = 0; i < 1000; i++) {
      tasks.add(new ZkTask("task" + i));
      client.submitTask(tasks.get(i));
    }

    int pre = 0;
    long preCount = 0;
    for (int i = 0; i < tasks.size(); i++) {
      long count = tasks.stream().filter(x -> x.done).count();
      System.out.println("finished tasks: " + count);
      if (i < masters.size() - 1) {
        masters.get(i).close();
      }

      if (i < workers.size() - 1) {
        workers.get(i).close();
      }

      Thread.sleep(1000);
      if (count == tasks.size()) {
        break;
      }
      if (preCount != count) {
        preCount = count;
        pre = i;
      } else {
        if (i - pre > 5) {
          break;
        }
      }

    }

    for (int i = 0; i < tasks.size(); i++) {
      Assert.assertTrue(tasks.get(i).name + " is not finished",
          tasks.get(i).done);
    }
  }
}