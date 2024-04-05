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

import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class TestCopyIntoFileLoadInfo {
  @Test
  public void testBuilder() {
    // Create ExtendedFormatOptions
    ExtendedFormatOptions formatOptions = getExtendedFormatOptions();

    // Create a CopyIntoFileLoadInfo using the Builder
    CopyIntoFileLoadInfo fileLoadInfo = getCopyIntoFileLoadInfo(formatOptions);

    // Verify the properties of the CopyIntoFileLoadInfo object
    assertThat("queryId").isEqualTo(fileLoadInfo.getQueryId());
    assertThat("queryUser").isEqualTo(fileLoadInfo.getQueryUser());
    assertThat("tableName").isEqualTo(fileLoadInfo.getTableName());
    assertThat("filePath").isEqualTo(fileLoadInfo.getFilePath());
    assertThat(100).isEqualTo(fileLoadInfo.getRecordsLoadedCount());
    assertThat(10).isEqualTo(fileLoadInfo.getRecordsRejectedCount());
    assertThat(12345L).isEqualTo(fileLoadInfo.getSnapshotId());
    assertThat("storageLocation").isEqualTo(fileLoadInfo.getStorageLocation());
    assertThat("JSON").isEqualTo(fileLoadInfo.getFileFormat());

    // Verify the formatOptions
    Map<CopyIntoTableContext.FormatOption, Object> expectedFormatOptions = getFormatOptions();

    assertThat(expectedFormatOptions).isEqualTo(fileLoadInfo.getFormatOptions());
  }

  @Test
  public void testSerializeAndDeserialize() {
    // Create ExtendedFormatOptions
    ExtendedFormatOptions formatOptions = getExtendedFormatOptions();

    // Create a CopyIntoFileLoadInfo using the Builder
    CopyIntoFileLoadInfo fileLoadInfo = getCopyIntoFileLoadInfo(formatOptions);

    // Serialize the CopyIntoErrorInfo to JSON
    String json = FileLoadInfo.Util.getJson(fileLoadInfo);

    // Deserialize the JSON back to a CopyIntoErrorInfo object
    CopyIntoFileLoadInfo deserializedFileLoadInfo =
        FileLoadInfo.Util.getInfo(json, CopyIntoFileLoadInfo.class);

    // Verify the properties of the deserialized CopyIntoErrorInfo object
    assertThat(fileLoadInfo.getQueryId()).isEqualTo(deserializedFileLoadInfo.getQueryId());
    assertThat(fileLoadInfo.getQueryUser()).isEqualTo(deserializedFileLoadInfo.getQueryUser());
    assertThat(fileLoadInfo.getTableName()).isEqualTo(deserializedFileLoadInfo.getTableName());
    assertThat(fileLoadInfo.getFilePath()).isEqualTo(deserializedFileLoadInfo.getFilePath());
    assertThat(fileLoadInfo.getRecordsLoadedCount())
        .isEqualTo(deserializedFileLoadInfo.getRecordsLoadedCount());
    assertThat(fileLoadInfo.getRecordsRejectedCount())
        .isEqualTo(deserializedFileLoadInfo.getRecordsRejectedCount());
    assertThat(fileLoadInfo.getSnapshotId()).isEqualTo(deserializedFileLoadInfo.getSnapshotId());
    assertThat(fileLoadInfo.getStorageLocation())
        .isEqualTo(deserializedFileLoadInfo.getStorageLocation());
    assertThat(fileLoadInfo.getFileFormat()).isEqualTo(deserializedFileLoadInfo.getFileFormat());

    // Verify the formatOptions
    Map<CopyIntoTableContext.FormatOption, Object> expectedFormatOptions = getFormatOptions();

    assertThat(expectedFormatOptions).isEqualTo(deserializedFileLoadInfo.getFormatOptions());
  }

  @Test
  public void testSerializeAndDeserializeFormatOptions() {
    // Create a map of format options
    Map<CopyIntoTableContext.FormatOption, Object> formatOptions = getFormatOptions();

    // Serialize the format options map to JSON
    String json = CopyIntoFileLoadInfo.Util.getJson(formatOptions);

    // Deserialize the JSON back to a map of format options
    Map<CopyIntoTableContext.FormatOption, Object> deserializedFormatOptions =
        CopyIntoFileLoadInfo.Util.getFormatOptions(json);

    // Verify the deserialized format options
    assertThat(formatOptions).isEqualTo(deserializedFormatOptions);
  }

  private static ExtendedFormatOptions getExtendedFormatOptions() {
    return new ExtendedFormatOptions(
        false,
        true,
        "yyyy-MM-dd",
        "HH:mm:ss",
        "YYYY-MM-DD HH24:MI:SS.FFF",
        ImmutableList.of("one", "two", "3", "IV"));
  }

  private static CopyIntoFileLoadInfo getCopyIntoFileLoadInfo(ExtendedFormatOptions formatOptions) {
    return new CopyIntoFileLoadInfo.Builder(
            "queryId",
            "queryUser",
            "tableName",
            "storageLocation",
            "filePath",
            formatOptions,
            FileType.JSON.name(),
            CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED)
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
