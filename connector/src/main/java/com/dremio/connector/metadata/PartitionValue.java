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
 * Interface for a connector to provide a value for a partition column. The caller is responsible for determining the
 * type of the column, and casting to the relevant sub-type, and then getting the value.
 * <p>
 * No type conversion is expected in implementations of this interface.
 */
public interface PartitionValue {

  /**
   * Enum describing types of partition values.
   */
  enum PartitionValueType {

    /**
     * ...
     */
    VISIBLE,

    /**
     * ...
     */
    IMPLICIT,

    /**
     * ...
     */
    INVISIBLE
  }

  /**
   * Get the partition value type.
   *
   * @return the partition value type, not null
   */
  default PartitionValueType getPartitionValueType() {
    return PartitionValueType.VISIBLE;
  }

  /**
   * Get the column name.
   *
   * @return the column name, not null
   */
  String getColumn();

  /**
   * If the value exists.
   *
   * @return true iff the value exists
   */
  default boolean hasValue() {
    return true;
  }

  /**
   * Boolean partition column value.
   */
  interface BooleanPartitionValue extends PartitionValue {

    /**
     * Get the boolean value.
     *
     * @return value
     */
    boolean getValue();

  }

  /**
   * Int partition column value.
   */
  interface IntPartitionValue extends PartitionValue {

    /**
     * Get the integer value.
     *
     * @return value
     */
    int getValue();

  }

  /**
   * Long partition column value.
   */
  interface LongPartitionValue extends PartitionValue {

    /**
     * Get the long value.
     *
     * @return value
     */
    long getValue();

  }

  /**
   * Float partition column value.
   */
  interface FloatPartitionValue extends PartitionValue {

    /**
     * Get the float value.
     *
     * @return value
     */
    float getValue();

  }

  /**
   * Double partition column value.
   */
  interface DoublePartitionValue extends PartitionValue {

    /**
     * Get the double value.
     *
     * @return value
     */
    double getValue();

  }

  /**
   * Binary partition column value.
   */
  interface BinaryPartitionValue extends PartitionValue {

    /**
     * Get the binary value.
     *
     * @return value
     */
    ByteBuffer getValue();

  }

  /**
   * String partition column value.
   */
  interface StringPartitionValue extends PartitionValue {

    /**
     * Get the string value.
     *
     * @return value
     */
    String getValue();

  }

  /**
   * Create {@code PartitionValue} with name and type, but no value.
   *
   * @param name partition column name
   * @return partition value
   */
  static PartitionValue of(String name) {
    return of(name, PartitionValueType.VISIBLE);
  }

  /**
   * Create {@code PartitionValue} with name and type, but no value.
   *
   * @param name partition column name
   * @param type partition value type
   * @return partition value
   */
  static PartitionValue of(String name, PartitionValueType type) {
    return new PartitionValues.NoValueImpl(name, type);
  }

  /**
   * Create string {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @return partition value
   */
  static PartitionValue of(String name, String value) {
    return of(name, value, PartitionValueType.VISIBLE);
  }

  /**
   * Create string {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @param type  partition value type
   * @return partition value
   */
  static PartitionValue of(String name, String value, PartitionValueType type) {
    return new PartitionValues.StringImpl(name, type, value);
  }

  /**
   * Create int {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @return partition value
   */
  static PartitionValue of(String name, int value) {
    return of(name, value, PartitionValueType.VISIBLE);
  }

  /**
   * Create int {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @param type  partition value type
   * @return partition value
   */
  static PartitionValue of(String name, int value, PartitionValueType type) {
    return new PartitionValues.IntImpl(name, type, value);
  }

  /**
   * Create double {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @return partition value
   */
  static PartitionValue of(String name, double value) {
    return of(name, value, PartitionValueType.VISIBLE);
  }

  /**
   * Create double {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @param type  partition value type
   * @return partition value
   */
  static PartitionValue of(String name, double value, PartitionValueType type) {
    return new PartitionValues.DoubleImpl(name, type, value);
  }

  /**
   * Create long {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @return partition value
   */
  static PartitionValue of(String name, long value) {
    return of(name, value, PartitionValueType.VISIBLE);
  }

  /**
   * Create long {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @param type  partition value type
   * @return partition value
   */
  static PartitionValue of(String name, long value, PartitionValueType type) {
    return new PartitionValues.LongImpl(name, type, value);
  }

  /**
   * Create float {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @return partition value
   */
  static PartitionValue of(String name, float value) {
    return of(name, value, PartitionValueType.VISIBLE);
  }

  /**
   * Create float {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @param type  partition value type
   * @return partition value
   */
  static PartitionValue of(String name, float value, PartitionValueType type) {
    return new PartitionValues.FloatImpl(name, type, value);
  }

  /**
   * Create boolean {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @return partition value
   */
  static PartitionValue of(String name, boolean value) {
    return of(name, value, PartitionValueType.VISIBLE);
  }

  /**
   * Create boolean {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @param type  partition value type
   * @return partition value
   */
  static PartitionValue of(String name, boolean value, PartitionValueType type) {
    return new PartitionValues.BooleanImpl(name, type, value);
  }

  /**
   * Create binary {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @return partition value
   */
  static PartitionValue of(String name, ByteBuffer value) {
    return of(name, value, PartitionValueType.VISIBLE);
  }

  /**
   * Create binary {@code PartitionValue} with name and type.
   *
   * @param name  partition column name
   * @param value partition column value
   * @param type  partition value type
   * @return partition value
   */
  static PartitionValue of(String name, ByteBuffer value, PartitionValueType type) {
    return new PartitionValues.BinaryImpl(name, type, value);
  }
}
