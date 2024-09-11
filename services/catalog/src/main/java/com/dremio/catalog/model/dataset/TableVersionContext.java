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
package com.dremio.catalog.model.dataset;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Version context for a table. Table version contexts support branch/tag/commit hash relative
 * versioning similar to the session scoped VersionContext, as well as table-specific versioning
 * either by snapshot ID or timestamp.
 *
 * <p>Expected values for each version type:
 *
 * <p>SNAPSHOT_ID/BRANCH/TAG/COMMIT/REFERENCE: string TIMESTAMP: long - for timestamp this is in
 * milliseconds since epoch
 */
public class TableVersionContext {
  private static final Logger logger = LoggerFactory.getLogger(TableVersionContext.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final TableVersionType type;
  private final Object value;
  private final Instant timestamp;

  // TODO: DX-85701
  // We should remove NOT_SPECIFIED (or disallow TableVersionContext to be null)
  public static final TableVersionContext NOT_SPECIFIED =
      new TableVersionContext(TableVersionType.NOT_SPECIFIED, "");

  @JsonCreator
  public TableVersionContext(
      @JsonProperty("type") TableVersionType type, @JsonProperty("value") Object value) {
    this.type = Preconditions.checkNotNull(type);
    this.value = validateTypeAndSpecifier(type, value);
    this.timestamp = null;
  }

  public TableVersionContext(TableVersionType type, Object value, Instant timestamp) {
    this.type = Preconditions.checkNotNull(type);
    this.value = validateTypeAndSpecifier(type, value);
    this.timestamp = timestamp;
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
   *
   * @return
   */
  public String toSql() {
    switch (type) {
      case BRANCH:
      case TAG:
      case COMMIT:
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
    return Objects.equals(type, other.type) && Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value);
  }

  private static Object validateTypeAndSpecifier(TableVersionType type, Object value) {
    switch (type) {
      case BRANCH:
      case TAG:
      case COMMIT:
      case REFERENCE:
      case NOT_SPECIFIED:
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
        if (timestamp != null) {
          return VersionContext.ofBranchAsOfTimestamp(getValueAs(String.class), timestamp);
        }
        return VersionContext.ofBranch(getValueAs(String.class));
      case TAG:
        if (timestamp != null) {
          return VersionContext.ofTagAsOfTimestamp(getValueAs(String.class), timestamp);
        }
        return VersionContext.ofTag(getValueAs(String.class));
      case COMMIT:
        return VersionContext.ofCommit(getValueAs(String.class));
      case REFERENCE:
        if (timestamp != null) {
          return VersionContext.ofRefAsOfTimestamp(getValueAs(String.class), timestamp);
        }
        return VersionContext.ofRef(getValueAs(String.class));
      case SNAPSHOT_ID:
      case NOT_SPECIFIED:
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
        return new TableVersionContext(
            TableVersionType.BRANCH, resolvedVersionContext.getRefName());
      case COMMIT:
        return new TableVersionContext(
            TableVersionType.COMMIT, resolvedVersionContext.getCommitHash());
      default:
        throw new IllegalStateException("Unexpected value: " + resolvedVersionContext.getType());
    }
  }

  public static TableVersionContext of(VersionContext versionContext) {
    Preconditions.checkNotNull(versionContext);
    switch (versionContext.getType()) {
      case TAG_AS_OF_TIMESTAMP:
        return new TableVersionContext(
            TableVersionType.TAG, versionContext.getValue(), versionContext.getTimestamp());
      case TAG:
        return new TableVersionContext(TableVersionType.TAG, versionContext.getValue());
      case BRANCH_AS_OF_TIMESTAMP:
        return new TableVersionContext(
            TableVersionType.BRANCH, versionContext.getValue(), versionContext.getTimestamp());
      case BRANCH:
        return new TableVersionContext(TableVersionType.BRANCH, versionContext.getValue());
      case COMMIT:
        return new TableVersionContext(TableVersionType.COMMIT, versionContext.getValue());
      case REF_AS_OF_TIMESTAMP:
        return new TableVersionContext(
            TableVersionType.REFERENCE, versionContext.getValue(), versionContext.getTimestamp());
      case REF:
        return new TableVersionContext(TableVersionType.REFERENCE, versionContext.getValue());
      case NOT_SPECIFIED:
        return NOT_SPECIFIED;
      default:
        throw new IllegalStateException("Unexpected value: " + versionContext.getType());
    }
  }

  public static TableVersionContext of(TimeTravelOption.TimeTravelRequest timeTravelRequest) {
    Preconditions.checkNotNull(timeTravelRequest);
    if (timeTravelRequest instanceof TimeTravelOption.TimestampRequest) {
      return new TableVersionContext(
          TableVersionType.TIMESTAMP,
          (((TimeTravelOption.TimestampRequest) timeTravelRequest).getTimestampMillis()));
    } else if (timeTravelRequest instanceof TimeTravelOption.SnapshotIdRequest) {
      return new TableVersionContext(
          (TableVersionType.SNAPSHOT_ID),
          ((TimeTravelOption.SnapshotIdRequest) timeTravelRequest).getSnapshotId());
    } else {
      throw new IllegalStateException("Unexpected value for TimeTravelRequest ");
    }
  }

  public String serialize() {
    String versionString = null;
    try {
      versionString = OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      logger.debug("Could not process table version context for {} ", this);
    }
    return versionString;
  }

  public static TableVersionContext deserialize(String versionString) {
    TableVersionContext tableVersionContext = null;
    try {
      tableVersionContext = OBJECT_MAPPER.readValue(versionString, TableVersionContext.class);
    } catch (JsonProcessingException e) {
      logger.debug("Invalid TableVersionContext string {}", versionString);
    }
    return tableVersionContext;
  }

  @JsonIgnore
  public boolean isTimeTravelType() {
    return type.isTimeTravel();
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
      case NOT_SPECIFIED:
      case BRANCH:
      case TAG:
      case COMMIT:
      case SNAPSHOT_ID:
        return Optional.of(new TableVersionContext(versionType, value));
      case TIMESTAMP:
        return Optional.of(new TableVersionContext(versionType, Long.valueOf(value)));
      default:
        return Optional.empty();
    }
  }
}
