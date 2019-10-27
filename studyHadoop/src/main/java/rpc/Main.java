package rpc;//package rpc;
//
//import java.io.IOException;
//
//public class Main {
//  public static void main(String[] args) throws InterruptedException {
//    new Thread(new Runnable() {
//      @Override
//      public void run() {
//        try {
//          MyServer.run();
//        } catch (IOException e) {
//          e.printStackTrace();
//        }
//      }
//    }).start();
//
//    Thread.sleep(1000);
//
//    new Thread(new Runnable() {
//      @Override
//      public void run() {
//        try {
//          MyClient.run();
//        } catch (IOException e) {
//          e.printStackTrace();
//        }
//      }
//    }).start();
//  }
//}
//
