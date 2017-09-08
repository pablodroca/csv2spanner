package ar.devfest.csv2spanner;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import ar.devfest.csv2spanner.database.ColumnValue;
import ar.devfest.csv2spanner.database.Row;
import ar.devfest.csv2spanner.database.SpannerService;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class MainApp {
  public static void main(String[] args) {
    System.out.println(String.format("Writing values to: %s -> %s.\n", args[0], args[1]));
    Reader data = new InputStreamReader(System.in);
    List<Row> rows = new ArrayList<>();
    try {
      CSVParser parser = CSVFormat.DEFAULT.parse(data);
      for(CSVRecord record : parser) {
        if (record.getRecordNumber() > 1) {
          String id = UUID.randomUUID().toString();
          Long value = Double.valueOf(record.get(0)).longValue();
          System.out.println(String.format("Parsing %s: %s", id, value));
          rows.add(new Row("tabletest").add(new ColumnValue("id", id)).add(new ColumnValue("value", value)));
        }
      }
    } catch (IOException e) {
      System.err.println("Unexpected error. The application will finish now. Error: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
    
    try(SpannerService spanner = new SpannerService(args[0], args[1])) {
      System.out.println(String.format("Writing %d total rows...", rows.size()));
      spanner.persist(rows);
      System.out.println(String.format("Rows written. Total: %d...", rows.size()));
    } catch (IOException e) {
      System.err.println("Unexpected error. The application will finish now. Error: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
