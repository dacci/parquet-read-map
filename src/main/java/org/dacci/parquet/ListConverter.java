package org.dacci.parquet;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.schema.GroupType;

final class ListConverter extends GroupConverterBase {
  @lombok.Getter(onMethod_ = @Override)
  private List<Object> currentRecord = null;

  ListConverter(final GroupConverterBase parent, final String name, final GroupType schema) {
    super(parent, name, schema);
  }

  @Override
  public void start() {
    currentRecord = new ArrayList<>();
  }

  @Override
  protected void add(final String name, final Object value) {
    currentRecord.add(value);
  }
}
