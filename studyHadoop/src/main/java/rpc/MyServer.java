package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class MyServer implements Runnable {
  @Override
  public void run() {
    RPC.Server server = null;
    try {
      server = new RPC.Builder(new Configuration())
          .setProtocol(ClientProtocol.class)
          .setInstance(new ClientProtocolImpl())
          .setBindAddress("localhost")
          .setPort(8000)
          .setNumHandlers(5).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
    server.start();
  }
}
