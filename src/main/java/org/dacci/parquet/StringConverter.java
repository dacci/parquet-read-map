package org.dacci.parquet;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

final class StringConverter extends PrimitiveConverter {
  private final GroupConverterBase parent;
  private final String name;

  StringConverter(final GroupConverterBase parent, final String name) {
    this.parent = parent;
    this.name = name;
  }

  @Override
  public void addBinary(final Binary value) {
    parent.add(name, value.toStringUsingUTF8());
  }
}
