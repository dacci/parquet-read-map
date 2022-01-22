package org.dacci.parquet;

import java.util.Map;

import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

class MapRecordMaterializer extends RecordMaterializer<Map<String, Object>> {
  @lombok.Getter(onMethod_ = @Override)
  private final MapConverter rootConverter;

  MapRecordMaterializer(final MessageType schema) {
    rootConverter = new MapConverter(null, null, schema);
  }

  @Override
  public Map<String, Object> getCurrentRecord() {
    return rootConverter.getCurrentRecord();
  }
}
