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

package com.dremio.exec.store.parquet;

import com.dremio.common.expression.CompleteType;

/** Field definition of the managed schema */
public class ManagedSchemaField {
  private final String name;
  private final String type;
  private final int length;
  private final int scale;
  private boolean isUnbounded;

  private ManagedSchemaField(
      final String name,
      final String type,
      final int length,
      final int scale,
      final boolean isUnbounded) {
    this.name = name;
    this.type = type;
    this.length = length;
    this.scale = scale;
    this.isUnbounded = isUnbounded;
    if (scale > length) {
      throw new IllegalStateException("Scale cannot be greater than precision");
    }
    if (!isUnbounded) {
      calculateIsUnbounded();
    }
  }

  public static ManagedSchemaField newUnboundedLenField(final String name, final String type) {
    return new ManagedSchemaField(name, type, getMaxFieldLen(type), 0, true);
  }

  public static ManagedSchemaField newFixedLenField(
      final String name, final String type, final int length, final int scale) {
    return new ManagedSchemaField(name, type, length, scale, false);
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public int getLength() {
    return length;
  }

  public int getScale() {
    return scale;
  }

  public boolean isTextField() {
    return isTextFieldType(type);
  }

  public boolean isUnbounded() {
    return isUnbounded;
  }

  private static boolean isTextFieldType(String type) {
    type = type.toLowerCase();
    return type.startsWith("varchar") || type.startsWith("char") || type.startsWith("string");
  }

  private static boolean isDecimalFieldType(final String type) {
    return type.toLowerCase().startsWith("decimal");
  }

  private static int getMaxFieldLen(final String type) {
    return isTextFieldType(type)
        ? CompleteType.DEFAULT_VARCHAR_PRECISION
        : isDecimalFieldType(type) ? CompleteType.MAX_DECIMAL_PRECISION : Integer.MAX_VALUE;
  }

  private void calculateIsUnbounded() {
    if (length <= 0 || length >= getMaxFieldLen(type)) {
      isUnbounded = true;
    }
  }

  @Override
  public String toString() {
    return "ManagedSchemaField{"
        + "name='"
        + name
        + '\''
        + ", type='"
        + type
        + '\''
        + ", length="
        + length
        + ", scale="
        + scale
        + '}';
  }
}
