import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class Main {
  public static void main(String[] args) throws IOException, WriteException {
    module();
    page();
    ui();
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

  public static void module() throws IOException, WriteException {
    WritableWorkbook wwb = Workbook.createWorkbook(new File("./module.xls"));
    WritableSheet ws = wwb.createSheet("module", 0);
    JsonNode jsonNode = new ObjectMapper().readTree(getStr("modules.txt"));
    jsonNode = jsonNode.get("body");
    ws.addCell(new Label(0, 0, "ID"));
    ws.addCell(new Label(1, 0, "moduleName"));
    ws.addCell(new Label(2, 0, "identifier"));
    ws.addCell(new Label(3, 0, "moduleType"));
    ws.addCell(new Label(4, 0, "tags"));
    ws.addCell(new Label(5, 0, "businessType"));
    ws.addCell(new Label(6, 0, "项目属性"));
    
    int index = 1;
    for (JsonNode node : jsonNode) {
      int j = 0;
      ws.addCell(new Label(j++, index, get(node, "id")));
      ws.addCell(new Label(j++, index, get(node,"moduleName").toString()));
      ws.addCell(new Label(j++, index,
          get(node, "moduleIdentifier").toString()));
      ws.addCell(new Label(j++, index, get(node, "moduleType").toString()));
      ws.addCell(new Label(j++, index, get(node, "tagDTOS").toString()));
      ws.addCell(new Label(j++, index, get(node, "businessType").toString()));
      ws.addCell(new Label(j++, index, get(node, "projectType")));
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
    ws.addCell(new Label(3, 0, "moduleType"));
    ws.addCell(new Label(4, 0, "tags"));
    ws.addCell(new Label(5, 0, "businessType"));
    ws.addCell(new Label(6, 0, "项目属性"));

    int index = 1;
    for (JsonNode node : jsonNode) {
      int j = 0;
      ws.addCell(new Label(j++, index, get(node, "id")));
      ws.addCell(new Label(j++, index, get(node,"moduleName").toString()));
      ws.addCell(new Label(j++, index,
          get(node, "moduleIdentifier").toString()));
      ws.addCell(new Label(j++, index, get(node, "moduleType").toString()));
      ws.addCell(new Label(j++, index, get(node, "tagDTOS").toString()));
      ws.addCell(new Label(j++, index, get(node, "businessType").toString()));
      ws.addCell(new Label(j++, index, get(node, "projectType")));
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
    ws.addCell(new Label(3, 0, "pageType"));
    ws.addCell(new Label(4, 0, "tags"));
    ws.addCell(new Label(5, 0, "businessType"));
    ws.addCell(new Label(6, 0, "项目属性"));

    int index = 1;
    for (JsonNode node : jsonNode) {
      int j = 0;
      ws.addCell(new Label(j++, index, get(node, "id")));
      ws.addCell(new Label(j++, index, get(node,"pageName").toString()));
      ws.addCell(new Label(j++, index,
          get(node, "pageIdentifier").toString()));
      ws.addCell(new Label(j++, index, get(node, "pageType").toString()));
      ws.addCell(new Label(j++, index, get(node, "tags").toString()));
      ws.addCell(new Label(j++, index, get(node, "businessType").toString()));
      ws.addCell(new Label(j++, index, get(node, "projectType")));
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
