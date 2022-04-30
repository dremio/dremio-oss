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
package com.dremio.exec.catalog;

import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * Version context for a table.  Table version contexts support branch/tag/commit hash relative versioning similar
 * to the session scoped VersionContext, as well as table-specific versioning either by snapshot ID or timestamp.
 *
 * Expected values for each version type:
 *
 * BRANCH/TAG/COMMIT/REFERENCE: string
 * SNAPSHOT_ID/TIMESTAMP: long - for timestamp this is in milliseconds since epoch
 */
public class TableVersionContext {
  private final TableVersionType type;
  private final TableVersionOperator operator;
  private final Object value;

  public static final TableVersionContext LATEST_VERSION =
    new TableVersionContext(TableVersionType.LATEST_VERSION, TableVersionOperator.AT, null);

  public TableVersionContext(TableVersionType type, TableVersionOperator operator, Object value) {
    this.type = Preconditions.checkNotNull(type);
    this.operator = Preconditions.checkNotNull(operator);
    this.value = validateTypeAndSpecifier(type, value);
  }

  public TableVersionType getType() {
    return type;
  }

  public TableVersionOperator getOperator() {
    return operator;
  }

  public Object getValue() {
    return value;
  }

  public <T> T getValueAs(Class<T> clazz) {
    return clazz.cast(value);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(operator);
    builder.append(" ");
    builder.append(type);
    if (value != null) {
      builder.append(" ");
      builder.append(value);
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || this.getClass() != obj.getClass()) {
      return false;
    }

    if (obj == this) {
      return true;
    }

    TableVersionContext other = (TableVersionContext) obj;
    return Objects.equals(type, other.type) &&
      Objects.equals(operator, other.operator) &&
      Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, operator, value);
  }

  private static Object validateTypeAndSpecifier(TableVersionType type, Object value) {
    switch (type) {
      case LATEST_VERSION:
        Preconditions.checkArgument(value == null);
        break;
      case BRANCH:
      case TAG:
      case COMMIT_HASH_ONLY:
      case REFERENCE:
      case SNAPSHOT_ID:
        Preconditions.checkArgument(value instanceof String);
        break;
      case TIMESTAMP:
        Preconditions.checkArgument(value instanceof Long);
        break;
      default:
        throw new AssertionError("Unsupported type " + type);
    }

    return value;
  }
}
