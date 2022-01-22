package org.dacci.parquet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;

final class DecimalConverter extends PrimitiveConverter {
  private final GroupConverterBase parent;
  private final String name;
  private final int scale;
  private final MathContext mathContext;

  DecimalConverter(
      final GroupConverterBase parent,
      final String name,
      final DecimalLogicalTypeAnnotation annotation) {
    this.parent = parent;
    this.name = name;
    scale = annotation.getScale();
    mathContext = new MathContext(annotation.getPrecision());
  }

  @Override
  public void addBinary(final Binary value) {
    parent.add(name, new BigDecimal(new BigInteger(value.getBytesUnsafe()), scale, mathContext));
  }

  @Override
  public void addInt(final int value) {
    addLong(value);
  }

  @Override
  public void addLong(final long value) {
    parent.add(name, new BigDecimal(BigInteger.valueOf(value), scale, mathContext));
  }
}
