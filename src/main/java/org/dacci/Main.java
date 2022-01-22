package org.dacci;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.dacci.parquet.MapReadSupport;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
  private static final ReadSupport<Map<String, Object>> READ_SUPPORT = MapReadSupport.getInstance();

  public static void main(final String... args) {
    if (args.length == 0) {
      parquetTesting();
    } else {
      for (final var arg : args) {
        read(arg);
      }
    }
  }

  private static void parquetTesting() {
    try (var stream = Files.newDirectoryStream(Paths.get("parquet-testing", "data"), "*.parquet")) {
      for (final var path : stream) {
        read(path.toString());
      }
    } catch (final IOException e) {
      log.error(null, e);
    }
  }

  private static void read(final String path) {
    log.info("Opening {}", path);
    try (var reader = ParquetReader.builder(READ_SUPPORT, new Path(path)).build()) {
      for (Map<String, Object> record = null; (record = reader.read()) != null; ) {
        log.info("{}", record);
      }
    } catch (IOException | RuntimeException e) {
      log.error(null, e);
    }
  }
}
