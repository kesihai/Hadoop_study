package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyClient implements Runnable {
  @Override
  public void run() {
    ClientProtocol proxy;
    try {
      proxy = RPC.getProxy(
          ClientProtocol.class,
          ClientProtocol.versionID,
          new InetSocketAddress("localhost", 8000),
          new Configuration());
      System.out.println(proxy.add(5, 6));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
