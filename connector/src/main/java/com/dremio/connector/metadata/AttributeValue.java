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

import java.util.List;

/**
 * Interface for a connector to provide a primitive value. The caller is responsible for determining
 * the type, and casting to the relevant sub-type, and then getting the value.
 *
 * <p>No type conversion is expected in implementations of this interface.
 */
public interface AttributeValue {

  Object getValueAsObject();

  /** Boolean primitive value. */
  interface BooleanValue extends AttributeValue {
    @Override
    default Object getValueAsObject() {
      return getValue();
    }

    /**
     * Get the boolean value.
     *
     * @return value
     */
    boolean getValue();
  }

  /** Long primitive value. */
  interface LongValue extends AttributeValue {
    @Override
    default Object getValueAsObject() {
      return getValue();
    }

    /**
     * Get the long value.
     *
     * @return value
     */
    long getValue();
  }

  /** Double primitive value. */
  interface DoubleValue extends AttributeValue {
    @Override
    default Object getValueAsObject() {
      return getValue();
    }

    /**
     * Get the double value.
     *
     * @return value
     */
    double getValue();
  }

  /** String primitive value. */
  interface StringValue extends AttributeValue {
    @Override
    default Object getValueAsObject() {
      return getValue();
    }

    /**
     * Get the string value.
     *
     * @return value
     */
    String getValue();

    boolean equalsIgnoreCase(AttributeValue.StringValue other);
  }

  /** Identifier primitive value */
  interface IdentifierValue extends AttributeValue {
    @Override
    default Object getValueAsObject() {
      return getComponents();
    }

    /**
     * Get components
     *
     * @return
     */
    List<String> getComponents();

    boolean equalsIgnoreCase(AttributeValue.IdentifierValue other);
  }

  /**
   * Create a boolean attribute value
   *
   * @param bool
   * @return
   */
  public static AttributeValue of(boolean bool) {
    return new AttributeValues.BooleanImpl(bool);
  }

  /**
   * Create a long attribute value
   *
   * @param longVal
   * @return
   */
  public static AttributeValue of(long longVal) {
    return new AttributeValues.LongImpl(longVal);
  }

  /**
   * Create a double attribute value
   *
   * @param doubleVal
   * @return
   */
  public static AttributeValue of(double doubleVal) {
    return new AttributeValues.DoubleImpl(doubleVal);
  }

  /**
   * Create a string attribute value
   *
   * @param stringVal
   * @return
   */
  public static AttributeValue of(String stringVal) {
    return new AttributeValues.StringImpl(stringVal);
  }

  /** Create an identifier attribute value */
  public static AttributeValue of(List<String> components) {
    return new AttributeValues.IdentifierImpl(components);
  }
}
