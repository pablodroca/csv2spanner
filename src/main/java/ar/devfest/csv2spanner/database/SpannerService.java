package ar.devfest.csv2spanner.database;

import com.google.cloud.Date;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.ValueBinder;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SpannerService implements Closeable {
  private Spanner spanner;
  private DatabaseClient dbClient;

  public SpannerService(String instance, String database) {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    this.spanner = options.getService();
    DatabaseId db = DatabaseId.of(options.getProjectId(), instance, database);
    this.dbClient = spanner.getDatabaseClient(db);
  }

  public ResultSet query(String statement) {
    return this.dbClient.singleUse().executeQuery(Statement.of(statement));
  }

  public void persist(Row row) {
    this.persist(Arrays.asList(row));
  }

  public void persist(Iterable<Row> rows) {
    List<Mutation> mutations = new ArrayList<>();
    for (Row row : rows) {
      Mutation mutation = this.buildMutation(row);
      mutations.add(mutation);
    }
    this.dbClient.write(mutations);
  }

  private Mutation buildMutation(Row row) {
    WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(row.getTableName());
    for (ColumnValue columnValue : row) {
      ValueBinder<WriteBuilder> valueBinder = builder.set(columnValue.getColumn());
      Object value = columnValue.getValue();
      if (value instanceof Long) {
        valueBinder.to((Long) value);
      } else if (value instanceof String) {
        valueBinder.to((String) value);
      } else if (value instanceof Double) {
        valueBinder.to((Double) value);
      } else if (value instanceof Boolean) {
        valueBinder.to((Boolean) value);
      } else if (value instanceof Date) {
        valueBinder.to((Date) value);
      }
    }
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    if (this.spanner != null) {
      this.spanner.close();
    }
  }
}
