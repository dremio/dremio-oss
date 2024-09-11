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
    assertThat(fileLoadInfo.getQueryId()).isEqualTo("queryId");
    assertThat(fileLoadInfo.getQueryUser()).isEqualTo("queryUser");
    assertThat(fileLoadInfo.getTableName()).isEqualTo("tableName");
    assertThat(fileLoadInfo.getFilePath()).isEqualTo("filePath");
    assertThat(fileLoadInfo.getRecordsLoadedCount()).isEqualTo(100);
    assertThat(fileLoadInfo.getRecordsRejectedCount()).isEqualTo(10);
    assertThat(fileLoadInfo.getSnapshotId()).isEqualTo(12345L);
    assertThat(fileLoadInfo.getStorageLocation()).isEqualTo("storageLocation");
    assertThat(fileLoadInfo.getFileFormat()).isEqualTo("JSON");
    assertThat(fileLoadInfo.getBranch()).isEqualTo("nessieBranch");
    assertThat(fileLoadInfo.getPipeName()).isEqualTo("dev-pipe");
    assertThat(fileLoadInfo.getPipeId()).isEqualTo("a79ed8c4-6581-4504-a5a9-5af6733b7f40");
    assertThat(fileLoadInfo.getProcessingStartTime())
        .isLessThanOrEqualTo(System.currentTimeMillis());
    assertThat(fileLoadInfo.getFileSize()).isEqualTo(45678L);
    assertThat(fileLoadInfo.getFirstErrorMessage()).isEqualTo("Unrecognized column type");
    assertThat(fileLoadInfo.getFileNotificationTimestamp())
        .isLessThanOrEqualTo(System.currentTimeMillis());
    assertThat(fileLoadInfo.getIngestionSourceType()).isEqualTo("AWS");
    assertThat(fileLoadInfo.getRequestId()).isEqualTo("5723df1f-3026-47b9-a089-d375d4008324");

    // Verify the formatOptions
    Map<CopyIntoTableContext.FormatOption, Object> expectedFormatOptions = getFormatOptions();

    assertThat(fileLoadInfo.getFormatOptions()).isEqualTo(expectedFormatOptions);
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
    assertThat(deserializedFileLoadInfo.getQueryId()).isEqualTo(fileLoadInfo.getQueryId());
    assertThat(deserializedFileLoadInfo.getQueryUser()).isEqualTo(fileLoadInfo.getQueryUser());
    assertThat(deserializedFileLoadInfo.getTableName()).isEqualTo(fileLoadInfo.getTableName());
    assertThat(deserializedFileLoadInfo.getFilePath()).isEqualTo(fileLoadInfo.getFilePath());
    assertThat(deserializedFileLoadInfo.getRecordsLoadedCount())
        .isEqualTo(fileLoadInfo.getRecordsLoadedCount());
    assertThat(deserializedFileLoadInfo.getRecordsRejectedCount())
        .isEqualTo(fileLoadInfo.getRecordsRejectedCount());
    assertThat(deserializedFileLoadInfo.getSnapshotId()).isEqualTo(fileLoadInfo.getSnapshotId());
    assertThat(deserializedFileLoadInfo.getStorageLocation())
        .isEqualTo(fileLoadInfo.getStorageLocation());
    assertThat(deserializedFileLoadInfo.getFileFormat()).isEqualTo(fileLoadInfo.getFileFormat());
    assertThat(deserializedFileLoadInfo.getBranch()).isEqualTo(fileLoadInfo.getBranch());
    assertThat(deserializedFileLoadInfo.getPipeName()).isEqualTo(fileLoadInfo.getPipeName());
    assertThat(deserializedFileLoadInfo.getPipeId()).isEqualTo(fileLoadInfo.getPipeId());
    assertThat(deserializedFileLoadInfo.getProcessingStartTime())
        .isEqualTo(fileLoadInfo.getProcessingStartTime());
    assertThat(deserializedFileLoadInfo.getFileSize()).isEqualTo(fileLoadInfo.getFileSize());
    assertThat(deserializedFileLoadInfo.getFirstErrorMessage())
        .isEqualTo(fileLoadInfo.getFirstErrorMessage());
    assertThat(deserializedFileLoadInfo.getFileNotificationTimestamp())
        .isEqualTo(fileLoadInfo.getFileNotificationTimestamp());
    assertThat(deserializedFileLoadInfo.getIngestionSourceType())
        .isEqualTo(fileLoadInfo.getIngestionSourceType());
    assertThat(deserializedFileLoadInfo.getRequestId()).isEqualTo(fileLoadInfo.getRequestId());

    // Verify the formatOptions
    Map<CopyIntoTableContext.FormatOption, Object> expectedFormatOptions = getFormatOptions();

    assertThat(deserializedFileLoadInfo.getFormatOptions()).isEqualTo(expectedFormatOptions);
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
        .setBranch("nessieBranch")
        .setPipeName("dev-pipe")
        .setPipeId("a79ed8c4-6581-4504-a5a9-5af6733b7f40")
        .setProcessingStartTime(System.currentTimeMillis())
        .setFileSize(45678L)
        .setFirstErrorMessage("Unrecognized column type")
        .setFileNotificationTimestamp(System.currentTimeMillis())
        .setIngestionSourceType("AWS")
        .setRequestId("5723df1f-3026-47b9-a089-d375d4008324")
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
