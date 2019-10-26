package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSClient {
  public static void upload(String source, String dest) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    fs.copyFromLocalFile(false, false, new Path(source), new Path(dest));
  }
}
