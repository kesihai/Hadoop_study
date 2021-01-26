package studySpark.sql.warehouse.po;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogItem {
  private String appName;
  private String mode;
}
