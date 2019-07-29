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

/**
 * Default implementations.
 */
final class PartitionValues {

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
  }

  /**
   * Binary implementation.
   */
  static final class BinaryImpl extends AbstractPartitionValue implements PartitionValue.BinaryPartitionValue {
    private final BytesOutput value;

    BinaryImpl(String name, PartitionValueType type, BytesOutput value) {
      super(name, type);
      this.value = value;
    }

    @Override
    public BytesOutput getValue() {
      return value;
    }
  }

  // prevent instantiation
  private PartitionValues() {
  }
}
