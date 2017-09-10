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
  private static final int IMPORT_BATCH_QTY = 20;

  private SpannerService spanner;
  private int importedRows;
  private ArrayList<Row> batchedRows = new ArrayList<>(IMPORT_BATCH_QTY);

  public Importer(SpannerService spanner) {
    this.spanner = spanner;
    this.importedRows = 0;
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
      Row row = new Row(entityName, true);
      ArrayList<String> values = new ArrayList<>(record.size());
      for (int i = 0; i < record.size(); i++) {
        String value = record.get(i);
        String propertyName = propertyNames.get(i);
        row.add(new ColumnValue(propertyName, value));
        values.add(value);
      }
      log.info(String.format("Storing %s(%s)", entityName, values));
      this.batchRow(row);
    }
    this.flushBatchedRows();
  }

  private void batchRow(Row row) {
    this.batchedRows.add(row);
    if (this.batchedRows.size() == IMPORT_BATCH_QTY) {
      this.flushBatchedRows();
    }
  }

  private void flushBatchedRows() {
    spanner.persist(this.batchedRows);
    this.importedRows += this.batchedRows.size();
    this.batchedRows.clear();
  }

  public int getImportedRows() {
    return importedRows;
  }
}
