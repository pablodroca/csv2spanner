package ar.devfest.csv2spanner;

import java.io.IOException;

import ar.devfest.csv2spanner.database.SpannerService;
import ar.devfest.csv2spanner.services.Importer;

public class MainApp {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Usage: java -jar csv2spanner.jar <spanner-instance> <database-name>.");
      System.exit(-1);
    }
    System.out.println(String.format("Writing values to: %s -> %s.\n", args[0], args[1]));

    try (SpannerService spanner = new SpannerService(args[0], args[1])) {
      Importer importer = new Importer(spanner);
      importer.importCSV("TestEntity", System.in);
      System.out.println(String.format("Rows written. Total: %d...", importer.getImportedRows()));
    } catch (IOException e) {
      System.err
          .println("Unexpected error. The application will finish now. Error: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
