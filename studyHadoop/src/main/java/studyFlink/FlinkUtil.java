package studyFlink;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkUtil {

  public static final String LOCALHOST = "localhost";

  public static DataStream<String> getSocketStream(StreamExecutionEnvironment env,
      String host, int port, String sep) {
    return env.socketTextStream(host, port)
        .flatMap(new FlatMapFunction<String, String>() {
          @Override
          public void flatMap(String value, Collector<String> out) throws Exception {
            String[] arr = value.split(sep);
            for (int i = 0; i < arr.length; i++) {
              if (!StringUtils.isEmpty(arr[i])) {
                out.collect(arr[i]);
              }
            }
          }
        });
  }
}
