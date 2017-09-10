package ar.devfest.csv2spanner.database;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class Row implements Iterable<ColumnValue> {
  public static final String DEFAULT_ID_COLUMNAME = "id";

  private String tableName;
  private List<ColumnValue> columnValues = new ArrayList<>();
  
  public Row(String tableName) {
    this(tableName, false);
  }

  public Row(String tableName, boolean includeId) {
    this.tableName = tableName;
    if (includeId) {
      String id = UUID.randomUUID().toString();
      this.columnValues.add(new ColumnValue(DEFAULT_ID_COLUMNAME, id));
    }
  }

  public String getTableName() {
    return tableName;
  }

  public Row add(ColumnValue columnValue) {
    this.columnValues.add(columnValue);
    return this;
  }

  @Override
  public Iterator<ColumnValue> iterator() {
    return this.columnValues.iterator();
  }
}
