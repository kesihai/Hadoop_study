import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import org.datanucleus.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Main {
  static Set<String> set = new HashSet<>();
  static Set<String> online;
  public static void main(String[] args) throws IOException, WriteException {
    init();
    print();
    module();
    page();
    ui();
  }

  public static void init() throws IOException {
    online = new HashSet<>();
    String str = str = Main.class.getClassLoader().getResource("a.txt").getPath();
    BufferedReader reader = new BufferedReader(new FileReader(new File(str)));
    while ((str = reader.readLine()) != null) {
      online.add(str);
    }
  }

  public static void print() throws IOException {
    JsonNode jsonNode = new ObjectMapper().readTree(getStr("modules.txt"));
    jsonNode = jsonNode.get("body");
    for (JsonNode node : jsonNode) {
      String owner = get(node, "moduleOwner");
      for (String str : owner.split(",")) {
        str = str.replace("\"", "");
        set.add(str);
      }
    }

    jsonNode = new ObjectMapper().readTree(getStr("ui.txt"));
    jsonNode = jsonNode.get("body");
    for (JsonNode node : jsonNode) {
      String owner = get(node, "moduleOwner");
      for (String str : owner.split(",")) {
        str = str.replace("\"", "");
        set.add(str);
      }
    }

    jsonNode = new ObjectMapper().readTree(getStr("pages.txt"));
    jsonNode = jsonNode.get("body");


    for (JsonNode node : jsonNode) {
      String owner = get(node, "pageOwner").replace("[", "").replace("]", "");
      for (String str : owner.split(",")) {
        str = str.replace("\"", "");
        set.add(str);
      }
    }
    set.removeIf(x -> StringUtils.isEmpty(x));
    System.out.println(set.size());
    System.out.println(set);
  }

  public static String getStr(String str) throws IOException {
    str = Main.class.getClassLoader().getResource(str).getPath();
    File file = new File(str);
    FileInputStream in = new FileInputStream(file);
    Long len = file.length();
    byte[] b = new byte[len.intValue()];
    String.valueOf(in.read(b));
    return new String(b);
  }

  public static boolean checkArray(List<String> list) {
    int count = 0;
    for (String str : list) {
      count += online.contains(str) ? 1 : 0;
      if (count > 1) return true;
    }
    return false;
  }

  public static void module() throws IOException, WriteException {
    WritableWorkbook wwb = Workbook.createWorkbook(new File("./module.xls"));
    WritableSheet ws = wwb.createSheet("module", 0);
    JsonNode jsonNode = new ObjectMapper().readTree(getStr("modules.txt"));
    jsonNode = jsonNode.get("body");
    ws.addCell(new Label(0, 0, "ID"));
    ws.addCell(new Label(1, 0, "moduleName"));
    ws.addCell(new Label(2, 0, "identifier"));
    ws.addCell(new Label(3, 0, "owner"));
    ws.addCell(new Label(4, 0, "creator"));
    ws.addCell(new Label(5, 0, "appName"));
    ws.addCell(new Label(6, 0, "businessType"));

//    ws.addCell(new Label(3, 0, "moduleType"));
//    ws.addCell(new Label(4, 0, "tags"));
//    ws.addCell(new Label(5, 0, "businessType"));
//    ws.addCell(new Label(6, 0, "项目属性"));
    
    int index = 1;
    for (JsonNode node : jsonNode) {

      List<String> list = new LinkedList<>();
      String owner = get(node, "moduleOwner");
      for (String str : owner.split(",")) {
        str = str.replace("\"", "");
        list.add(str);
      }
      if (!checkArray(list)) {
        continue;
      }

      int j = 0;
      ws.addCell(new Label(j++, index, get(node, "id")));
      ws.addCell(new Label(j++, index, get(node,"moduleName")));
      ws.addCell(new Label(j++, index, get(node, "moduleIdentifier")));
      ws.addCell(new Label(j++, index, get(node, "moduleOwner")));
      ws.addCell(new Label(j++, index, get(node, "creator")));
      ws.addCell(new Label(j++, index, get(node, "appName")));
      ws.addCell(new Label(j++, index, get(node, "businessType")));
//      ws.addCell(new Label(j++, index, get(node, "moduleType")));
//      ws.addCell(new Label(j++, index, get(node, "tagDTOS")));
//      ws.addCell(new Label(j++, index, get(node, "businessType")));
//      ws.addCell(new Label(j++, index, get(node, "projectType")));
      index++;
    }
    wwb.write();
    wwb.close();
  }

  public static void ui() throws IOException, WriteException {
    WritableWorkbook wwb = Workbook.createWorkbook(new File("./ui.xls"));
    WritableSheet ws = wwb.createSheet("module", 0);
    JsonNode jsonNode = new ObjectMapper().readTree(getStr("ui.txt"));
    jsonNode = jsonNode.get("body");
    ws.addCell(new Label(0, 0, "ID"));
    ws.addCell(new Label(1, 0, "moduleName"));
    ws.addCell(new Label(2, 0, "identifier"));
    ws.addCell(new Label(3, 0, "owner"));
    ws.addCell(new Label(4, 0, "creator"));
    ws.addCell(new Label(5, 0, "appName"));
    ws.addCell(new Label(6, 0, "businessType"));
//    ws.addCell(new Label(3, 0, "moduleType"));
//    ws.addCell(new Label(4, 0, "tags"));
//    ws.addCell(new Label(5, 0, "businessType"));
//    ws.addCell(new Label(6, 0, "项目属性"));

    int index = 1;
    for (JsonNode node : jsonNode) {

      List<String> list = new LinkedList<>();
      String owner = get(node, "moduleOwner");
      for (String str : owner.split(",")) {
        str = str.replace("\"", "");
        list.add(str);
      }
      if (!checkArray(list)) {
        continue;
      }


      int j = 0;
      ws.addCell(new Label(j++, index, get(node, "id")));
      ws.addCell(new Label(j++, index, get(node,"moduleName")));
      ws.addCell(new Label(j++, index, get(node, "moduleIdentifier")));
      ws.addCell(new Label(j++, index, get(node, "moduleOwner")));
      ws.addCell(new Label(j++, index, get(node, "creator")));
      ws.addCell(new Label(j++, index, get(node, "appName")));
      ws.addCell(new Label(j++, index, get(node, "businessType")));
//      ws.addCell(new Label(j++, index, get(node, "moduleType").toString()));
//      ws.addCell(new Label(j++, index, get(node, "tagDTOS").toString()));
//      ws.addCell(new Label(j++, index, get(node, "businessType").toString()));
//      ws.addCell(new Label(j++, index, get(node, "projectType")));

      index++;
    }
    wwb.write();
    wwb.close();
  }

  public static void page() throws IOException, WriteException {
    WritableWorkbook wwb = Workbook.createWorkbook(new File("./page.xls"));
    WritableSheet ws = wwb.createSheet("page", 1);
    JsonNode jsonNode = new ObjectMapper().readTree(getStr("pages.txt"));
    jsonNode = jsonNode.get("body");

    ws.addCell(new Label(0, 0, "ID"));
    ws.addCell(new Label(1, 0, "pageName"));
    ws.addCell(new Label(2, 0, "identifier"));
    ws.addCell(new Label(3, 0, "owner"));
    ws.addCell(new Label(4, 0, "creator"));
    ws.addCell(new Label(5, 0, "appName"));
    ws.addCell(new Label(6, 0, "businessType"));

//    ws.addCell(new Label(3, 0, "pageType"));
//    ws.addCell(new Label(4, 0, "tags"));
//    ws.addCell(new Label(5, 0, "businessType"));
//    ws.addCell(new Label(6, 0, "项目属性"));

    int index = 1;
    for (JsonNode node : jsonNode) {
      List<String> list = new LinkedList<>();
      String owner = get(node, "pageOwner").replace("[", "").replace("]", "");
      for (String str : owner.split(",")) {
        str = str.replace("\"", "");
        list.add(str);
      }
      if (!checkArray(list)) {
        continue;
      }

      int j = 0;
      ws.addCell(new Label(j++, index, get(node, "id")));
      ws.addCell(new Label(j++, index, get(node,"pageName").toString()));
      ws.addCell(new Label(j++, index,
          get(node, "pageIdentifier").toString()));
      ws.addCell(new Label(j++, index, get(node, "pageOwner")));
      ws.addCell(new Label(j++, index, get(node, "creator")));
      ws.addCell(new Label(j++, index, get(node, "appName")));
      ws.addCell(new Label(j++, index, get(node, "businessType")));
//      ws.addCell(new Label(j++, index, get(node, "pageType").toString()));
//      ws.addCell(new Label(j++, index, get(node, "tags").toString()));
//      ws.addCell(new Label(j++, index, get(node, "businessType").toString()));
//      ws.addCell(new Label(j++, index, get(node, "projectType")));

      index++;
    }
    wwb.write();
    wwb.close();
  }

  public static String get(JsonNode node, String key) {
    if (!node.has(key)) {
      return "";
    }
    return node.get(key).toString();
  }
}
