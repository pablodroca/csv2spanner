package ar.devfest.csv2spanner.database;

public class ColumnValue {
  private String column;
  private Object value;
  
  public ColumnValue(String column, Object value) {
    super();
    this.column = column;
    this.value = value;
  }

  public String getColumn() {
    return column;
  }
  public void setColumn(String column) {
    this.column = column;
  }
  public Object getValue() {
    return value;
  }
  public void setValue(Object value) {
    this.value = value;
  }

}
