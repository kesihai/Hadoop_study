package utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import studySpark.sql.warehouse.po.LogItem;

import java.util.LinkedList;
import java.util.List;

public class CommonUtil {
  public static String getPath(String path) {
    return CommonUtil.class.getClassLoader().getResource(path).getPath();
  }

  public static class WareHouseLogUtil {
    public static List<LogItem> string2List(String log) {
      int index = log.indexOf("[{");
      if (index == -1) {
        return new LinkedList<>();
      }
      log = log.substring(index);
      JsonArray arr = new JsonParser().parse(log).getAsJsonArray();
      List<LogItem> list = new LinkedList<>();
      for (int i = 0; i < arr.size(); i++) {
        JsonObject obj = arr.get(i).getAsJsonObject();
        if (!obj.has("source")) {
          continue;
        }
        obj = obj.getAsJsonObject("source");
        list.add(new LogItem(obj.get("app").getAsString(), obj.get("mode").getAsString()));
      }
      return list;
    }
  }
}
