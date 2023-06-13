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

import java.sql.Timestamp;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.connector.metadata.options.TimeTravelOption;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Version context for a table.  Table version contexts support branch/tag/commit hash relative versioning similar
 * to the session scoped VersionContext, as well as table-specific versioning either by snapshot ID or timestamp.
 *
 * Expected values for each version type:
 *
 * SNAPSHOT_ID/BRANCH/TAG/COMMIT/REFERENCE: string
 * TIMESTAMP: long - for timestamp this is in milliseconds since epoch
 */
public class TableVersionContext {
  private static final Logger logger = LoggerFactory.getLogger(TableVersionContext.class);
  private final TableVersionType type;
  private final Object value;

  public static final TableVersionContext LATEST_VERSION =
    new TableVersionContext(TableVersionType.LATEST_VERSION, null);

  @JsonCreator
  public TableVersionContext(@JsonProperty("type") TableVersionType type,
                             @JsonProperty("value") Object value) {
    this.type = Preconditions.checkNotNull(type);
    this.value = validateTypeAndSpecifier(type, value);
  }

  public TableVersionType getType() {
    return type;
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
    builder.append(type);
    if (value != null) {
      builder.append(" ");
      builder.append(value);
    }
    return builder.toString();
  }

  /**
   * Converts the TableVersionContext into a valid SQL expression
   * @return
   */
  public String toSql() {
    switch (type) {
      case BRANCH:
      case TAG:
      case COMMIT_HASH_ONLY:
      case REFERENCE:
        return String.format("%s \"%s\"", type.toSqlRepresentation(), value);
      case TIMESTAMP:
        Timestamp ts = new Timestamp(Long.valueOf(value.toString()));
        return String.format("%s '%s'", type.toSqlRepresentation(), ts);
      case SNAPSHOT_ID:
        return String.format("%s '%s'", type.toSqlRepresentation(), value);
      default:
        throw new IllegalStateException(String.format("Unable to convert %s to sql", type));
    }
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
      Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value);
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

  public VersionContext asVersionContext() {
    switch (type) {
      case BRANCH:
        return VersionContext.ofBranch(getValueAs(String.class));
      case TAG:
        return VersionContext.ofTag(getValueAs(String.class));
      case COMMIT_HASH_ONLY:
        return VersionContext.ofBareCommit(getValueAs(String.class));
      case REFERENCE:
        return VersionContext.ofRef(getValueAs(String.class));
      case LATEST_VERSION:
      case SNAPSHOT_ID:
      case TIMESTAMP:
        return VersionContext.NOT_SPECIFIED;
      default:
        throw new AssertionError("Unsupported type " + type);
    }
  }

  public static TableVersionContext of(ResolvedVersionContext resolvedVersionContext) {
    Preconditions.checkNotNull(resolvedVersionContext);
    switch (resolvedVersionContext.getType()) {
      case TAG:
        return new TableVersionContext(TableVersionType.TAG, resolvedVersionContext.getRefName());
      case BRANCH:
        return new TableVersionContext(TableVersionType.BRANCH, resolvedVersionContext.getRefName());
      case BARE_COMMIT:
        return new TableVersionContext(TableVersionType.COMMIT_HASH_ONLY, resolvedVersionContext.getCommitHash());
      default:
        throw new IllegalStateException("Unexpected value: " + resolvedVersionContext.getType());
    }
  }

  public static TableVersionContext of(VersionContext versionContext) {
    Preconditions.checkNotNull(versionContext);
    switch (versionContext.getType()) {
      case TAG:
        return new TableVersionContext(TableVersionType.TAG, versionContext.getValue());
      case BRANCH:
        return new TableVersionContext(TableVersionType.BRANCH, versionContext.getValue());
      case BARE_COMMIT:
        return new TableVersionContext(TableVersionType.COMMIT_HASH_ONLY, versionContext.getValue());
      case REF:
        return new TableVersionContext(TableVersionType.REFERENCE, versionContext.getValue());
      case UNSPECIFIED:
        return LATEST_VERSION;
      default:
        throw new IllegalStateException("Unexpected value: " + versionContext.getType());
    }
  }

  public static TableVersionContext of(TimeTravelOption.TimeTravelRequest timeTravelRequest) {
    Preconditions.checkNotNull(timeTravelRequest);
    if (timeTravelRequest instanceof TimeTravelOption.TimestampRequest) {
      return new TableVersionContext(TableVersionType.TIMESTAMP,
        (((TimeTravelOption.TimestampRequest) timeTravelRequest).getTimestampMillis()));
    } else if (timeTravelRequest instanceof TimeTravelOption.SnapshotIdRequest) {
      return new TableVersionContext((TableVersionType.SNAPSHOT_ID),
        ((TimeTravelOption.SnapshotIdRequest) timeTravelRequest).getSnapshotId());
    } else {
      throw new IllegalStateException("Unexpected value for TimeTravelRequest ");
    }
  }

  public String serialize() {
    ObjectMapper om = new ObjectMapper();
    String versionString = null;
    try {
      versionString = om.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      logger.debug("Could not process  table version context for {} ", this);
    }
    return versionString;
  }

  public static TableVersionContext deserialize(String versionString) {
    TableVersionContext tableVersionContext = null;
    ObjectMapper om = new ObjectMapper();
    try {
      tableVersionContext = om.readValue(versionString, TableVersionContext.class);
    } catch (JsonProcessingException e) {
      logger.debug("Invalid TableVersionContext string {}", versionString);
    }
    return tableVersionContext;
  }

  boolean isTimeTravelType() {
    return type == TableVersionType.TIMESTAMP || type == TableVersionType.SNAPSHOT_ID;
  }

  public static Optional<TableVersionContext> tryParse(String type, String value) {
    if (Strings.isNullOrEmpty(type) || Strings.isNullOrEmpty(value)) {
      return Optional.empty();
    }

    final TableVersionType versionType = TableVersionType.getType(type.toUpperCase());

    if (versionType == null) {
      return Optional.empty();
    }

    switch (versionType) {
      case BRANCH:
      case TAG:
      case COMMIT_HASH_ONLY:
      case SNAPSHOT_ID:
        return Optional.of(new TableVersionContext(versionType, value));
      case TIMESTAMP:
        return Optional.of(new TableVersionContext(versionType, Long.valueOf(value)));
      default:
        return Optional.empty();
    }
  }
}
