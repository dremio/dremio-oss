/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.planner.logical;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Util class for determining if two RelDataTypes are equal under certain conditions. For example,
 * we can compare them with and without the nullability and precision.
 */
public final class RelDataTypeEqualityComparer {
  private RelDataTypeEqualityComparer() {}

  public static boolean areEqual(RelDataType first, RelDataType second) {
    return areEquals(first, second, Options.DEFAULT);
  }

  public static boolean areEquals(RelDataType first, RelDataType second, Options options) {
    if (Objects.equals(first, second)) {
      return true;
    }

    if (options.matchAnyToAll
        && (first.getSqlTypeName() == SqlTypeName.ANY
            || second.getSqlTypeName() == SqlTypeName.ANY)) {
      return true;
    }

    if (first.getSqlTypeName() != second.getSqlTypeName()) {
      return false;
    }

    if (options.considerNullability && (first.isNullable() != second.isNullable())) {
      return false;
    }

    if (options.considerPrecision
        && first.getSqlTypeName().allowsPrec()
        && (first.getPrecision() != second.getPrecision())) {
      return false;
    }

    if (options.considerScale
        && first.getSqlTypeName().allowsScale()
        && (first.getScale() != second.getScale())) {
      return false;
    }

    if (options.considerCharset && !Objects.equals(first.getCharset(), second.getCharset())) {
      return false;
    }

    if (options.considerCollation && !Objects.equals(first.getCollation(), second.getCollation())) {
      return false;
    }

    if (options.considerIntervalQualifier
        && !Objects.equals(first.getIntervalQualifier(), second.getIntervalQualifier())) {
      return false;
    }

    if (first.isStruct()) {
      List<RelDataTypeField> firstFieldList = first.getFieldList();
      List<RelDataTypeField> secondFieldList = second.getFieldList();

      if (firstFieldList.size() != secondFieldList.size()) {
        return false;
      }

      for (int i = 0; i < firstFieldList.size(); i++) {
        RelDataTypeField firstField = firstFieldList.get(i);
        RelDataTypeField secondField = secondFieldList.get(i);

        if (options.considerFieldNames
            && !Objects.equals(firstField.getName(), secondField.getName())) {
          return false;
        }

        if (options.considerFieldNameOrder && (firstField.getIndex() != secondField.getIndex())) {
          return false;
        }

        if (!areEquals(firstField.getType(), secondField.getType(), options)) {
          return false;
        }
      }
    }

    if (!areEquals(first.getComponentType(), second.getComponentType(), options)) {
      return false;
    }

    if (!areEquals(first.getKeyType(), second.getKeyType(), options)) {
      return false;
    }

    if (!areEquals(first.getValueType(), second.getValueType(), options)) {
      return false;
    }

    return true;
  }

  public static final class Options {
    public static final Options DEFAULT = Options.builder().build();
    private final boolean considerNullability;
    private final boolean considerPrecision;
    private final boolean considerScale;
    private final boolean considerCharset;
    private final boolean considerCollation;
    private final boolean considerIntervalQualifier;
    private final boolean considerFieldNames;
    private final boolean considerFieldNameOrder;
    private final boolean matchAnyToAll;

    private Options(Builder builder) {
      this.considerNullability = builder.considerNullability;
      this.considerPrecision = builder.considerPrecision;
      this.considerScale = builder.considerScale;
      this.considerCharset = builder.considerCharset;
      this.considerCollation = builder.considerCollation;
      this.considerIntervalQualifier = builder.considerIntervalQualifier;
      this.considerFieldNames = builder.considerFieldNames;
      this.considerFieldNameOrder = builder.considerFieldNameOrder;
      this.matchAnyToAll = builder.matchAnyToAll;
    }

    public static Builder builder() {
      return new Builder();
    }

    public static final class Builder {
      private boolean considerNullability;
      private boolean considerPrecision;
      private boolean considerScale;
      private boolean considerCharset;
      private boolean considerCollation;
      private boolean considerIntervalQualifier;
      private boolean considerFieldNames;
      private boolean considerFieldNameOrder;
      private boolean matchAnyToAll;

      private Builder() {
        // Set default values if needed
        this.considerNullability = true;
        this.considerPrecision = true;
        this.considerScale = true;
        this.considerCharset = true;
        this.considerCollation = true;
        this.considerIntervalQualifier = true;
        this.considerFieldNames = true;
        this.considerFieldNameOrder = true;
        this.matchAnyToAll = false;
      }

      public Builder withConsiderNullability(boolean considerNullability) {
        this.considerNullability = considerNullability;
        return this;
      }

      public Builder withConsiderPrecision(boolean considerPrecision) {
        this.considerPrecision = considerPrecision;
        return this;
      }

      public Builder withConsiderScale(boolean considerScale) {
        this.considerScale = considerScale;
        return this;
      }

      public Builder withConsiderCharset(boolean considerCharset) {
        this.considerCharset = considerCharset;
        return this;
      }

      public Builder withConsiderCollation(boolean considerCollation) {
        this.considerCollation = considerCollation;
        return this;
      }

      public Builder withConsiderIntervalQualifier(boolean considerIntervalQualifier) {
        this.considerIntervalQualifier = considerIntervalQualifier;
        return this;
      }

      public Builder withConsiderFieldNames(boolean considerFieldNames) {
        this.considerFieldNames = considerFieldNames;
        return this;
      }

      public Builder withConsiderFieldNameOrder(boolean considerFieldNameOrder) {
        this.considerFieldNameOrder = considerFieldNameOrder;
        return this;
      }

      public Builder withMatchAnyToAll(boolean matchAnyToAll) {
        this.matchAnyToAll = matchAnyToAll;
        return this;
      }

      public Options build() {
        return new Options(this);
      }
    }
  }
}
