package ar.devfest.csv2spanner.services;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import ar.devfest.csv2spanner.database.ColumnValue;
import ar.devfest.csv2spanner.database.Row;
import ar.devfest.csv2spanner.database.SpannerService;

public class Importer {
  private static final Logger log = Logger.getLogger(Importer.class.getName());
  private SpannerService spanner;
  private int importedRows;

  public Importer(SpannerService spanner) {
    this.spanner = spanner;
    this.setImportedRows(0);
  }

  public void importCSV(String entityName, InputStream in) throws IOException {
    log.info(String.format("Parsing CSV for entity:%s", entityName));
    Reader data = new InputStreamReader(in);
    CSVParser parser = CSVFormat.DEFAULT.parse(data);
    Iterator<CSVRecord> it = parser.iterator();
    ArrayList<String> propertyNames = new ArrayList<>();
    if (it.hasNext()) {
      CSVRecord header = it.next();
      for (String propertyName : header) {
        propertyNames.add(propertyName);
      }
    }
    log.info(String.format("Header properties detected:%s", propertyNames));
    while (it.hasNext()) {
      CSVRecord record = it.next();
      Row row = new Row(entityName);
      ArrayList<String> values = new ArrayList<>(record.size());
      for (int i = 0; i < record.size(); i++) {
        String value = record.get(i);
        String propertyName = propertyNames.get(i);
        row.add(new ColumnValue(propertyName, value));
        values.add(value);
      }
      log.info(String.format("Storing %s(%s)", entityName, values));
      spanner.persist(row);
      this.importedRows++;
    }
  }

  public int getImportedRows() {
    return importedRows;
  }
}
