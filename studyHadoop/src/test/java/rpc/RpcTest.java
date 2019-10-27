package rpc;

import org.junit.Test;

public class RpcTest {
  public static void main(String[] args) {
    new Thread(new MyServer()).start();
    new Thread(new MyClient()).start();
  }
}
