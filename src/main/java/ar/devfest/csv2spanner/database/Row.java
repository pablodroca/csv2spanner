package ar.devfest.csv2spanner.database;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Row implements Iterable<ColumnValue> {
  private String tableName;
  private List<ColumnValue> columnValues = new ArrayList<>();
  
  public Row(String tableName) {
    this.tableName = tableName;
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
