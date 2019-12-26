package studyzk.taskAssign;

import java.util.concurrent.CountDownLatch;

/**
 * ZkTask.
 */
public class ZkTask {
  String task;
  String name;
  boolean done = false;
  boolean succeed = false;
  private CountDownLatch latch = new CountDownLatch(1);

  public ZkTask(String task) {
    this.task = task;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setStatus(boolean status) {
    done = true;
    succeed = status;
    latch.countDown();
  }

  public void waitUntilDone() {
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
