package org.dacci.parquet;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class MapReadSupport extends ReadSupport<Map<String, Object>> {
  private static class Holder {
    public static final MapReadSupport INSTANCE = new MapReadSupport();
  }

  public static ReadSupport<Map<String, Object>> getInstance() {
    return Holder.INSTANCE;
  }

  MapReadSupport() {}

  @Override
  public ReadContext init(final InitContext context) {
    return new ReadContext(context.getFileSchema());
  }

  @Override
  public RecordMaterializer<Map<String, Object>> prepareForRead(
      final Configuration configuration,
      final Map<String, String> keyValueMetaData,
      final MessageType fileSchema,
      final ReadContext readContext) {
    return new MapRecordMaterializer(fileSchema);
  }
}
