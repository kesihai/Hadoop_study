package studyHive.unittest;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class HiveTestHelloWorld {
  
  @HiveSQL(files = {})
  private HiveShell shell;

  @BeforeEach
  public void setupSourceDatabase() {

  }

  @Test
  public void testMaxValueByYear() {
    /*
     * Insert some source data
     */
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.test_table (")
        .append("year STRING, value INT")
        .append(")")
        .toString());
    shell.insertInto("source_db", "test_table")
        .withColumns("year", "value")
        .addRow("2014", 3)
        .addRow("2014", 4)
        .addRow("2015", 2)
        .addRow("2015", 5)
        .commit();

    List<Object[]> result = shell.executeStatement(
        "select * from source_db.test_table");

    assertEquals(4, result.size());
    assertArrayEquals(new Object[]{"2014", 3}, result.get(0));
    assertArrayEquals(new Object[]{"2014", 4}, result.get(1));
  }

}
