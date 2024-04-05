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

/** Default implementations. */
public final class AttributeValues {

  private static final int PRIME = 31;

  /** Abstract implementation. */
  private abstract static class AbstractAttributeValue implements AttributeValue {

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException("Not expected to call this");
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      return obj.getClass() == getClass();
    }
  }

  /** String implementation. */
  static final class StringImpl extends AbstractAttributeValue
      implements AttributeValue.StringValue {
    private final String value;

    StringImpl(String value) {
      this.value = value;
    }

    @Override
    public String getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return (value == null ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      AttributeValues.StringImpl other = (AttributeValues.StringImpl) obj;
      return value == null ? other.value == null : value.equals(other.value);
    }

    @Override
    public boolean equalsIgnoreCase(AttributeValue.StringValue other) {
      if (!super.equals(other)) {
        return false;
      }
      AttributeValues.StringImpl otherImpl = (AttributeValues.StringImpl) other;
      return value == null ? otherImpl.value == null : value.equalsIgnoreCase(otherImpl.value);
    }
  }

  /** Boolean implementation. */
  static final class BooleanImpl extends AbstractAttributeValue
      implements AttributeValue.BooleanValue {
    private final boolean value;

    BooleanImpl(boolean value) {
      this.value = value;
    }

    @Override
    public boolean getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return Boolean.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      return value == ((AttributeValues.BooleanImpl) obj).value;
    }
  }

  /** Double implementation. */
  static final class DoubleImpl extends AbstractAttributeValue
      implements AttributeValue.DoubleValue {
    private final double value;

    DoubleImpl(double value) {
      this.value = value;
    }

    @Override
    public double getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return Double.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      return value == ((AttributeValues.DoubleImpl) obj).value;
    }
  }

  /** Long implementation. */
  static final class LongImpl extends AbstractAttributeValue implements AttributeValue.LongValue {
    private final long value;

    LongImpl(long value) {
      this.value = value;
    }

    @Override
    public long getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      return value == ((AttributeValues.LongImpl) obj).value;
    }
  }

  /** Identifier implementation */
  static final class IdentifierImpl extends AbstractAttributeValue
      implements AttributeValue.IdentifierValue {

    private final List<String> components;

    IdentifierImpl(final List<String> components) {
      this.components = components;
    }

    @Override
    public List<String> getComponents() {
      return components;
    }

    @Override
    public int hashCode() {
      return components.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!super.equals(obj)) {
        return false;
      }
      return components.equals(((IdentifierImpl) obj).components);
    }

    @Override
    public boolean equalsIgnoreCase(AttributeValue.IdentifierValue obj) {
      if (!super.equals(obj)) {
        return false;
      }
      List<String> theirComponents = ((IdentifierImpl) obj).components;

      if (components.size() != theirComponents.size()) {
        return false;
      }

      for (int i = 0; i < components.size(); i++) {
        if (!components.get(i).equalsIgnoreCase(theirComponents.get(i))) {
          return false;
        }
      }
      return true;
    }
  }

  // prevent instantiation
  private AttributeValues() {}
}
