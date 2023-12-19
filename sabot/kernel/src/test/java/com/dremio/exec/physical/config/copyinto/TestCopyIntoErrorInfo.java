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


import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.store.dfs.ErrorInfo;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestCopyIntoErrorInfo {
  @Test
  public void testBuilder() {
    // Create ExtendedFormatOptions
    ExtendedFormatOptions formatOptions = getExtendedFormatOptions();

    // Create a CopyIntoErrorInfo using the Builder
    CopyIntoErrorInfo errorInfo = getCopyIntoErrorInfo(formatOptions);

    // Verify the properties of the CopyIntoErrorInfo object
    assertThat("queryId").isEqualTo(errorInfo.getQueryId());
    assertThat("queryUser").isEqualTo(errorInfo.getQueryUser());
    assertThat("tableName").isEqualTo(errorInfo.getTableName());
    assertThat("filePath").isEqualTo(errorInfo.getFilePath());
    assertThat(100).isEqualTo(errorInfo.getRecordsLoadedCount());
    assertThat(10).isEqualTo(errorInfo.getRecordsRejectedCount());
    assertThat(12345L).isEqualTo(errorInfo.getSnapshotId());
    assertThat("storageLocation").isEqualTo(errorInfo.getStorageLocation());
    assertThat("JSON").isEqualTo(errorInfo.getFileFormat());

    // Verify the formatOptions
    Map<CopyIntoTableContext.FormatOption, Object> expectedFormatOptions = getFormatOptions();

    assertThat(expectedFormatOptions).isEqualTo(errorInfo.getFormatOptions());
  }

  @Test
  public void testSerializeAndDeserialize() {
    // Create ExtendedFormatOptions
    ExtendedFormatOptions formatOptions = getExtendedFormatOptions();

    // Create a CopyIntoErrorInfo using the Builder
    CopyIntoErrorInfo errorInfo = getCopyIntoErrorInfo(formatOptions);

    // Serialize the CopyIntoErrorInfo to JSON
    String json = ErrorInfo.Util.getJson(errorInfo);

    // Deserialize the JSON back to a CopyIntoErrorInfo object
    CopyIntoErrorInfo deserializedErrorInfo = ErrorInfo.Util.getInfo(json, CopyIntoErrorInfo.class);

    // Verify the properties of the deserialized CopyIntoErrorInfo object
    assertThat(errorInfo.getQueryId()).isEqualTo(deserializedErrorInfo.getQueryId());
    assertThat(errorInfo.getQueryUser()).isEqualTo(deserializedErrorInfo.getQueryUser());
    assertThat(errorInfo.getTableName()).isEqualTo(deserializedErrorInfo.getTableName());
    assertThat(errorInfo.getFilePath()).isEqualTo(deserializedErrorInfo.getFilePath());
    assertThat(errorInfo.getRecordsLoadedCount()).isEqualTo(deserializedErrorInfo.getRecordsLoadedCount());
    assertThat(errorInfo.getRecordsRejectedCount()).isEqualTo(deserializedErrorInfo.getRecordsRejectedCount());
    assertThat(errorInfo.getSnapshotId()).isEqualTo(deserializedErrorInfo.getSnapshotId());
    assertThat(errorInfo.getStorageLocation()).isEqualTo(deserializedErrorInfo.getStorageLocation());
    assertThat(errorInfo.getFileFormat()).isEqualTo(deserializedErrorInfo.getFileFormat());

    // Verify the formatOptions
    Map<CopyIntoTableContext.FormatOption, Object> expectedFormatOptions = getFormatOptions();

    assertThat(expectedFormatOptions).isEqualTo(deserializedErrorInfo.getFormatOptions());
  }

  @Test
  public void testSerializeAndDeserializeFormatOptions() {
    // Create a map of format options
    Map<CopyIntoTableContext.FormatOption, Object> formatOptions = getFormatOptions();

    // Serialize the format options map to JSON
    String json = CopyIntoErrorInfo.Util.getJson(formatOptions);

    // Deserialize the JSON back to a map of format options
    Map<CopyIntoTableContext.FormatOption, Object> deserializedFormatOptions = CopyIntoErrorInfo.Util.getFormatOptions(json);

    // Verify the deserialized format options
    assertThat(formatOptions).isEqualTo(deserializedFormatOptions);
  }

  private static ExtendedFormatOptions getExtendedFormatOptions() {
    return new ExtendedFormatOptions(false, true, "yyyy-MM-dd",
      "HH:mm:ss", "YYYY-MM-DD HH24:MI:SS.FFF", ImmutableList.of("one", "two", "3", "IV"));
  }

  private static CopyIntoErrorInfo getCopyIntoErrorInfo(ExtendedFormatOptions formatOptions) {
    return new CopyIntoErrorInfo.Builder("queryId", "queryUser", "tableName", "storageLocation",
      "filePath", formatOptions, FileType.JSON.name(), CopyIntoErrorInfo.CopyIntoFileState.PARTIALLY_LOADED)
      .setRecordsLoadedCount(100)
      .setRecordsRejectedCount(10)
      .setRecordDelimiter("\n")
      .setFieldDelimiter(",")
      .setQuoteChar("\"")
      .setEscapeChar("\\")
      .setSnapshotId(12345L)
      .build();
  }

  private static Map<CopyIntoTableContext.FormatOption, Object> getFormatOptions() {
    return ImmutableMap.of(
      CopyIntoTableContext.FormatOption.TRIM_SPACE, false,
      CopyIntoTableContext.FormatOption.DATE_FORMAT, "yyyy-MM-dd",
      CopyIntoTableContext.FormatOption.TIME_FORMAT, "HH:mm:ss",
      CopyIntoTableContext.FormatOption.EMPTY_AS_NULL, true,
      CopyIntoTableContext.FormatOption.TIMESTAMP_FORMAT, "YYYY-MM-DD HH24:MI:SS.FFF",
      CopyIntoTableContext.FormatOption.NULL_IF, ImmutableList.of("one", "two", "3", "IV"),
      CopyIntoTableContext.FormatOption.RECORD_DELIMITER, "\n",
      CopyIntoTableContext.FormatOption.FIELD_DELIMITER, ",",
      CopyIntoTableContext.FormatOption.QUOTE_CHAR, "\"",
      CopyIntoTableContext.FormatOption.ESCAPE_CHAR, "\\");
  }
}
