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
package com.dremio.connector.metadata;

import java.nio.ByteBuffer;

/**
 * Default implementations.
 */
final class PartitionValues {
  static final int PRIME = 31;

  /**
   * Abstract implementation.
   */
  private static class AbstractPartitionValue implements PartitionValue {
    private final String name;
    private final PartitionValueType type;

    AbstractPartitionValue(String name, PartitionValueType type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public PartitionValueType getPartitionValueType() {
      return type;
    }

    @Override
    public String getColumn() {
      return name;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = PRIME * result + ((name == null) ? 0 : name.hashCode());
      result = PRIME * result + ((type == null) ? 0 : type.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      if (obj.getClass() != getClass()) {
        return false;
      }

      AbstractPartitionValue other = (AbstractPartitionValue)obj;
      return type.equals(other.type) && name.equals(other.name);
    }
  }

  /**
   * No value implementation.
   */
  static final class NoValueImpl extends AbstractPartitionValue {

    NoValueImpl(String name, PartitionValueType type) {
      super(name, type);
    }

    @Override
    public boolean hasValue() {
      return false;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }
  }

  /**
   * String implementation.
   */
  static final class StringImpl extends AbstractPartitionValue implements PartitionValue.StringPartitionValue {
    private final String value;

    StringImpl(String name, PartitionValueType type, String value) {
      super(name, type);
      this.value = value;
    }

    @Override
    public String getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return PRIME * super.hashCode() + (value == null ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      StringImpl other = (StringImpl)obj;
      return value == null ? other.value == null : value.equals(other.value);
    }
  }

  /**
   * Int implementation.
   */
  static final class IntImpl extends AbstractPartitionValue implements PartitionValue.IntPartitionValue {
    private final int value;

    IntImpl(String name, PartitionValueType type, int value) {
      super(name, type);
      this.value = value;
    }

    @Override
    public int getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return PRIME * super.hashCode() + Integer.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      return value == ((IntImpl)obj).value;
    }
  }

  /**
   * Double implementation.
   */
  static final class DoubleImpl extends AbstractPartitionValue implements PartitionValue.DoublePartitionValue {
    private final double value;

    DoubleImpl(String name, PartitionValueType type, double value) {
      super(name, type);
      this.value = value;
    }

    @Override
    public double getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return PRIME * super.hashCode() + Double.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      return value == ((DoubleImpl)obj).value;
    }
  }

  /**
   * Long implementation.
   */
  static final class LongImpl extends AbstractPartitionValue implements PartitionValue.LongPartitionValue {
    private final long value;

    LongImpl(String name, PartitionValueType type, long value) {
      super(name, type);
      this.value = value;
    }

    @Override
    public long getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return PRIME * super.hashCode() + Long.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      return value == ((LongImpl)obj).value;
    }
  }

  /**
   * Float implementation.
   */
  static final class FloatImpl extends AbstractPartitionValue implements PartitionValue.FloatPartitionValue {
    private final float value;

    FloatImpl(String name, PartitionValueType type, float value) {
      super(name, type);
      this.value = value;
    }

    @Override
    public float getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return PRIME * super.hashCode() + Float.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      return value == ((FloatImpl)obj).value;
    }
  }

  /**
   * Boolean implementation.
   */
  static final class BooleanImpl extends AbstractPartitionValue implements PartitionValue.BooleanPartitionValue {
    private final boolean value;

    BooleanImpl(String name, PartitionValueType type, boolean value) {
      super(name, type);
      this.value = value;
    }

    @Override
    public boolean getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return PRIME * super.hashCode() + Boolean.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      return value == ((BooleanImpl)obj).value;
    }
  }

  /**
   * Binary implementation.
   */
  static final class BinaryImpl extends AbstractPartitionValue implements PartitionValue.BinaryPartitionValue {
    private final ByteBuffer value;

    BinaryImpl(String name, PartitionValueType type, ByteBuffer value) {
      super(name, type);
      this.value = value;
    }

    @Override
    public ByteBuffer getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return PRIME * super.hashCode() + value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }

      BinaryImpl other = (BinaryImpl)obj;
      return value == null ? other.value == null : value.equals(other.value);
    }
  }

  // prevent instantiation
  private PartitionValues() {
  }
}
