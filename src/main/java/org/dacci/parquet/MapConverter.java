package org.dacci.parquet;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.parquet.schema.GroupType;

final class MapConverter extends GroupConverterBase {
  @lombok.Getter(onMethod_ = @Override)
  private Map<String, Object> currentRecord = null;

  MapConverter(final GroupConverterBase parent, final String name, final GroupType schema) {
    super(parent, name, schema);
  }

  @Override
  public void start() {
    currentRecord = new LinkedHashMap<>();
  }

  @Override
  protected void add(final String name, final Object value) {
    currentRecord.put(name, value);
  }
}
