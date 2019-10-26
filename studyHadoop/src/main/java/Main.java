import utils.HDFSClient;

import java.io.IOException;
import java.net.URISyntaxException;

public class Main {
  public static void main(String[] args) throws IOException {
    String source = args[0];
    String dest = args[1];
    HDFSClient.upload(source, dest);
  }
}
