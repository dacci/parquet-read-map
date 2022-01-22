package org.dacci.parquet;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

abstract sealed class GroupConverterBase extends GroupConverter
    permits ListConverter, MapConverter {
  private final GroupConverterBase parent;
  private final String name;
  private final List<Converter> converters;

  protected GroupConverterBase(
      final GroupConverterBase parent, final String name, final GroupType schema) {
    this.parent = parent;
    this.name = name;
    this.converters =
        schema.getFields().stream()
            .map(this::createConverter)
            .collect(Collectors.toUnmodifiableList());
  }

  private Converter createConverter(final Type type) {
    return type.isPrimitive()
        ? createConverter(type.asPrimitiveType())
        : createConverter(type.asGroupType());
  }

  private Converter createConverter(final PrimitiveType type) {
    final var logicalType = type.getLogicalTypeAnnotation();
    if (logicalType == null) {
      return switch (type.getPrimitiveTypeName()) {
        case BINARY, FIXED_LEN_BYTE_ARRAY -> new StringConverter(this, type.getName());
        case INT96 -> new InstantConverter(this, type.getName(), TimeUnit.MILLIS);
        default -> new NumberConverter(this, type.getName(), true);
      };
    }

    return logicalType
        .accept(
            new LogicalTypeAnnotationVisitor<Converter>() {
              @Override
              public Optional<Converter> visit(StringLogicalTypeAnnotation stringLogicalType) {
                return Optional.of(new StringConverter(GroupConverterBase.this, type.getName()));
              }

              @Override
              public Optional<Converter> visit(DecimalLogicalTypeAnnotation decimalLogicalType) {
                return Optional.of(
                    new DecimalConverter(
                        GroupConverterBase.this, type.getName(), decimalLogicalType));
              }

              @Override
              public Optional<Converter> visit(
                  TimestampLogicalTypeAnnotation timestampLogicalType) {
                return Optional.of(
                    new InstantConverter(
                        GroupConverterBase.this, type.getName(), timestampLogicalType.getUnit()));
              }

              @Override
              public Optional<Converter> visit(IntLogicalTypeAnnotation intLogicalType) {
                return Optional.of(
                    new NumberConverter(
                        GroupConverterBase.this, type.getName(), intLogicalType.isSigned()));
              }
            })
        .get();
  }

  private Converter createConverter(final GroupType type) {
    return switch (type.getRepetition()) {
      case REPEATED -> new ListConverter(this, type.getName(), type);
      default -> new MapConverter(this, type.getName(), type);
    };
  }

  @Override
  public Converter getConverter(final int fieldIndex) {
    return converters.get(fieldIndex);
  }

  @Override
  public void end() {
    if (parent != null) {
      parent.add(name, getCurrentRecord());
    }
  }

  abstract Object getCurrentRecord();

  abstract void add(String name, Object value);
}
