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
package com.dremio.exec.physical.config.copyinto;

import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.FormatOption;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class represents the information of a file that was loaded during the COPY INTO operation.
 * It implements the {@link FileLoadInfo} interface.
 */
public final class CopyIntoFileLoadInfo implements FileLoadInfo {

  /**
   * The {@code CopyFileState} enumeration represents the state of a file during a copy operation.
   */
  public enum CopyIntoFileState {
    PARTIALLY_LOADED,
    SKIPPED,
    FULLY_LOADED,
    IN_PROGRESS
  }

  private String queryId;
  private String queryUser;
  private String tableName;
  private String storageLocation;
  private String filePath;
  private Map<FormatOption, Object> formatOptions;
  private long recordsLoadedCount;
  private long recordsRejectedCount;
  private Long snapshotId;
  private String fileFormat;
  private CopyIntoFileState fileState;

  private CopyIntoFileLoadInfo() {}

  private CopyIntoFileLoadInfo(Builder builder) {
    this.queryId = builder.queryId;
    this.queryUser = builder.queryUser;
    this.tableName = builder.tableName;
    this.storageLocation = builder.storageLocation;
    this.filePath = builder.filePath;
    this.formatOptions = builder.getFormatOptions();
    this.recordsLoadedCount = builder.recordsLoadedCount;
    this.recordsRejectedCount = builder.recordsRejectedCount;
    this.snapshotId = builder.snapshotId;
    this.fileFormat = builder.fileFormat;
    this.fileState = builder.fileState;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getQueryUser() {
    return queryUser;
  }

  public String getTableName() {
    return tableName;
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  public String getFilePath() {
    return filePath;
  }

  public Map<FormatOption, Object> getFormatOptions() {
    return formatOptions;
  }

  public long getRecordsLoadedCount() {
    return recordsLoadedCount;
  }

  public long getRecordsRejectedCount() {
    return recordsRejectedCount;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  public String getFileFormat() {
    return fileFormat;
  }

  public CopyIntoFileState getFileState() {
    return fileState;
  }

  /** Builder class for constructing CopyIntoFileLoadInfo instances. */
  public static final class Builder {
    private final String queryId;
    private final String queryUser;
    private final String tableName;
    private final String storageLocation;
    private final String filePath;
    private ExtendedFormatOptions formatOptions;
    private long recordsLoadedCount;
    private long recordsRejectedCount;
    private String recordDelimiter;
    private String fieldDelimiter;
    private String quoteChar;
    private String escapeChar;
    private Long snapshotId;
    private Map<FormatOption, Object> formatOptionsMap;
    private final String fileFormat;
    private final CopyIntoFileState fileState;

    /**
     * Constructs a new instance of the CopyIntoFileLoadInfo.Builder with the given parameters.
     *
     * @param queryId The query ID associated with the error.
     * @param queryUser The user who executed the query.
     * @param tableName The name of the target table.
     * @param storageLocation The storage location of the input files
     * @param filePath The file path where the error occurred.
     * @param formatOptions The format options associated with copy into query.
     * @param fileFormat The format of the input file (JSON, CSV, Parquet etc.)
     * @param fileState The loading state of the input file (LOADED, SKIPPED, PARTIALLY_LOADED etc.)
     */
    public Builder(
        String queryId,
        String queryUser,
        String tableName,
        String storageLocation,
        String filePath,
        ExtendedFormatOptions formatOptions,
        String fileFormat,
        CopyIntoFileState fileState) {
      this.queryId = queryId;
      this.queryUser = queryUser;
      this.tableName = tableName;
      this.storageLocation = storageLocation;
      this.filePath = filePath;
      this.formatOptions = formatOptions;
      this.fileFormat = fileFormat;
      this.fileState = fileState;
    }

    /**
     * Constructs a new instance of the CopyIntoFileLoadInfo.Builder by copying the properties from
     * an existing CopyIntoFileLoadInfo instance.
     *
     * @param info The existing CopyIntoFileLoadInfo instance to copy properties from.
     */
    public Builder(CopyIntoFileLoadInfo info) {
      queryId = info.getQueryId();
      queryUser = info.getQueryUser();
      tableName = info.getTableName();
      storageLocation = info.getStorageLocation();
      filePath = info.filePath;
      recordsLoadedCount = info.getRecordsLoadedCount();
      recordsRejectedCount = info.getRecordsRejectedCount();
      snapshotId = info.getSnapshotId();
      formatOptionsMap = info.getFormatOptions();
      fileFormat = info.getFileFormat();
      fileState = info.getFileState();
    }

    /**
     * Private helper method to construct the map of format options used during COPY INTO operation.
     * This map will be created lazily to avoid unnecessary object creation.
     *
     * @return The map of format options.
     */
    private Map<FormatOption, Object> getFormatOptions() {
      if (formatOptionsMap != null) {
        return formatOptionsMap;
      }

      formatOptionsMap = new LinkedHashMap<>();
      formatOptionsMap.put(FormatOption.TRIM_SPACE, formatOptions.getTrimSpace());
      formatOptionsMap.put(FormatOption.DATE_FORMAT, formatOptions.getDateFormat());
      formatOptionsMap.put(FormatOption.TIME_FORMAT, formatOptions.getTimeFormat());
      formatOptionsMap.put(FormatOption.EMPTY_AS_NULL, formatOptions.getEmptyAsNull());
      formatOptionsMap.put(FormatOption.TIMESTAMP_FORMAT, formatOptions.getTimeStampFormat());
      formatOptionsMap.put(FormatOption.NULL_IF, formatOptions.getNullIfExpressions());
      formatOptionsMap.put(FormatOption.RECORD_DELIMITER, recordDelimiter);
      formatOptionsMap.put(FormatOption.FIELD_DELIMITER, fieldDelimiter);
      formatOptionsMap.put(FormatOption.QUOTE_CHAR, quoteChar);
      formatOptionsMap.put(FormatOption.ESCAPE_CHAR, escapeChar);
      return formatOptionsMap;
    }

    // Setters for the class properties

    public Builder setRecordsLoadedCount(long recordsLoadedCount) {
      this.recordsLoadedCount = recordsLoadedCount;
      return this;
    }

    public Builder setRecordsRejectedCount(long recordsRejectedCount) {
      this.recordsRejectedCount = recordsRejectedCount;
      return this;
    }

    public Builder setRecordDelimiter(String recordDelimiter) {
      this.recordDelimiter = recordDelimiter;
      return this;
    }

    public Builder setFieldDelimiter(String fieldDelimiter) {
      this.fieldDelimiter = fieldDelimiter;
      return this;
    }

    public Builder setQuoteChar(String quoteChar) {
      this.quoteChar = quoteChar;
      return this;
    }

    public Builder setEscapeChar(String escapeChar) {
      this.escapeChar = escapeChar;
      return this;
    }

    public Builder setSnapshotId(Long snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    /**
     * Builds a new instance of CopyIntoFileLoadInfo with the properties set in this builder.
     *
     * @return The constructed CopyIntoFileLoadInfo instance.
     */
    public CopyIntoFileLoadInfo build() {
      return new CopyIntoFileLoadInfo(this);
    }
  }

  /**
   * The Util class provides utility methods for serializing and deserializing format options to and
   * from JSON. It contains static methods to format options map to their JSON representation and
   * vice versa using Jackson ObjectMapper. The methods handle exceptions related to JSON processing
   * and throw IllegalStateException if serialization or deserialization fails.
   */
  public static final class Util {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Util() {}

    /**
     * Serializes the given format options map to its JSON representation.
     *
     * @param formatOptions The map of format options to be serialized.
     * @return The JSON representation of the format options map.
     * @throws IllegalStateException If serialization to JSON fails.
     */
    public static String getJson(Map<FormatOption, Object> formatOptions) {
      try {
        return MAPPER.writeValueAsString(formatOptions);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(
            String.format("Cannot serialize format option map to json: %s", formatOptions));
      }
    }

    /**
     * Deserializes the given JSON string to a map of format options.
     *
     * @param json The JSON string to be deserialized.
     * @return The map of format options.
     * @throws IllegalStateException If deserialization from JSON fails.
     */
    public static Map<FormatOption, Object> getFormatOptions(String json) {
      TypeReference<Map<FormatOption, Object>> typeRef =
          new TypeReference<Map<FormatOption, Object>>() {};
      try {
        return MAPPER.readValue(json, typeRef);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(
            String.format("Cannot deserialize json to format option map:\n%s", json));
      }
    }
  }
}
