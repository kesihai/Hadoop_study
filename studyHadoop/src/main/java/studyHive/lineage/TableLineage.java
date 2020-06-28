package studyHive.lineage;

import jodd.io.FileUtil;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.tools.LineageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TableLineage {
  private static final Logger LOG = LoggerFactory.getLogger(TableLineage.class);
  private static LineageInfo lineageInfo = new LineageInfo();

  public static void getLineage(String query) throws SemanticException,
      ParseException {
    lineageInfo.getLineageInfo(query);
    LOG.info("input tables:");
    LOG.info(lineageInfo.getInputTableList().toString());
    LOG.info("output tables:");
    LOG.info(lineageInfo.getOutputTableList().toString());
  }

  public static void main(String[] args) {
    try {
      String query = FileUtil.readString("src/main/resources/sql/a.sql");
      LOG.info(query);
      getLineage(query);
    } catch (IOException | SemanticException | ParseException e) {
      e.printStackTrace();
    }
  }
}
