package org.dacci.parquet;

import java.nio.ByteOrder;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;

final class InstantConverter extends PrimitiveConverter {
  private static final long JULIAN_DAY_OF_EPOCH = 2_440_588;
  private static final long SECONDS_PER_DAY = 86_400;

  private final GroupConverterBase parent;
  private final String name;
  private final TimeUnit unit;

  InstantConverter(
      final GroupConverterBase parent,
      final String name,
      final LogicalTypeAnnotation.TimeUnit unit) {
    this.parent = parent;
    this.name = name;
    this.unit =
        switch (unit) {
          case MILLIS -> TimeUnit.MILLISECONDS;
          case MICROS -> TimeUnit.MICROSECONDS;
          case NANOS -> TimeUnit.NANOSECONDS;
        };
  }

  @Override
  public void addBinary(final Binary value) {
    final var buffer = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
    final var nanoseconds = buffer.getLong();
    final var days = Integer.toUnsignedLong(buffer.getInt());
    final var seconds = (days - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;

    parent.add(name, Instant.ofEpochSecond(seconds, nanoseconds));
  }

  @Override
  public void addInt(final int value) {
    addLong(value);
  }

  @Override
  public void addLong(final long value) {
    parent.add(name, Instant.ofEpochMilli(unit.toMillis(value)));
  }
}
