package org.dacci.parquet;

import java.math.BigInteger;

import org.apache.parquet.io.api.PrimitiveConverter;

final class NumberConverter extends PrimitiveConverter {
  private final GroupConverterBase parent;
  private final String name;
  private final boolean signed;

  NumberConverter(final GroupConverterBase parent, final String name, final boolean signed) {
    this.parent = parent;
    this.name = name;
    this.signed = signed;
  }

  @Override
  public void addBoolean(final boolean value) {
    parent.add(name, value);
  }

  @Override
  public void addDouble(final double value) {
    parent.add(name, value);
  }

  @Override
  public void addFloat(final float value) {
    parent.add(name, value);
  }

  @Override
  public void addInt(final int value) {
    parent.add(name, signed ? value : Integer.toUnsignedLong(value));
  }

  @Override
  public void addLong(final long value) {
    parent.add(name, signed ? value : new BigInteger(Long.toUnsignedString(value)));
  }
}
