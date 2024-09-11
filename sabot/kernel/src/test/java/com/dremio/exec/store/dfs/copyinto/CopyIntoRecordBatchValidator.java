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
package com.dremio.exec.store.dfs.copyinto;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.exec.physical.config.copyinto.CopyIntoFileLoadInfo;
import com.dremio.exec.store.dfs.FileLoadInfo;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.RecordBatchValidatorDefaultImpl;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.RecordSet.RsRecord;
import de.vandermeer.asciitable.v2.V2_AsciiTable;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyIntoRecordBatchValidator extends RecordBatchValidatorDefaultImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(CopyIntoRecordBatchValidator.class);

  public CopyIntoRecordBatchValidator(RecordSet recordSet) {
    super(recordSet);
  }

  @Override
  protected boolean compareRecord(
      RsRecord expected, List<ValueVector> actual, int actualIndex, V2_AsciiTable output) {
    assertThat(actual.size()).isEqualTo(expected.getValues().length);
    boolean matches = true;
    Object[] outputValues = new Object[actual.size()];
    for (int c = 0; c < actual.size(); c++) {
      FieldReader reader = actual.get(c).getReader();
      reader.setPosition(actualIndex);
      boolean columnMatches;
      if (recordSet
          .getSchema()
          .getFields()
          .get(c)
          .getName()
          .equalsIgnoreCase(ColumnUtils.COPY_HISTORY_COLUMN_NAME)) {
        columnMatches = compareCopyHistoryColumnValue((String) expected.getValues()[c], reader);
      } else {
        columnMatches = compareValue(expected.getValues()[c], reader);
      }
      reader.setPosition(actualIndex);
      String actualText = actualToString(reader);
      if (columnMatches) {
        outputValues[c] = actualText;
      } else {
        matches = false;
        outputValues[c] =
            String.format("%s (%s)", actualText, expectedToString(expected.getValues()[c]));
      }
    }

    output.addRow(outputValues);
    output.addRule();

    return matches;
  }

  private boolean compareCopyHistoryColumnValue(String expectedValue, FieldReader fileReader) {
    if (!fileReader.isSet() || expectedValue == null) {
      return !fileReader.isSet() && expectedValue == null;
    }
    String actualValue = fileReader.readObject().toString();
    CopyIntoFileLoadInfo expectedInfo =
        FileLoadInfo.Util.getInfo(expectedValue, CopyIntoFileLoadInfo.class);
    CopyIntoFileLoadInfo actualInfo =
        FileLoadInfo.Util.getInfo(actualValue, CopyIntoFileLoadInfo.class);
    if (actualInfo.getRecordsRejectedCount() != expectedInfo.getRecordsRejectedCount()) {
      logValueMismatchError(
          "Rejected record count",
          actualInfo.getRecordsRejectedCount(),
          expectedInfo.getRecordsRejectedCount());
      return false;
    }
    if (actualInfo.getRecordsLoadedCount() != expectedInfo.getRecordsLoadedCount()) {
      logValueMismatchError(
          "Record loaded count",
          actualInfo.getRecordsLoadedCount(),
          expectedInfo.getRecordsLoadedCount());

      return false;
    }
    if (!actualInfo.getTableName().equals(expectedInfo.getTableName())) {
      logValueMismatchError("Table name", actualInfo.getTableName(), expectedInfo.getTableName());
      return false;
    }
    if (actualInfo.getFilePath() == null || actualInfo.getFilePath().isEmpty()) {
      logValueMismatchError("File path", actualInfo.getFilePath(), expectedInfo.getFilePath());
      return false;
    }
    if (!actualInfo.getQueryUser().equals(expectedInfo.getQueryUser())) {
      logValueMismatchError("Query user", actualInfo.getQueryUser(), expectedInfo.getQueryUser());
      return false;
    }
    if (!actualInfo.getQueryId().equals(expectedInfo.getQueryId())) {
      logValueMismatchError("Query Id", actualInfo.getQueryId(), expectedInfo.getQueryId());
      return false;
    }
    if (!actualInfo.getFileState().equals(expectedInfo.getFileState())) {
      logValueMismatchError("File state", actualInfo.getFileState(), expectedInfo.getFileState());
      return false;
    }
    if (!Objects.equals(actualInfo.getBranch(), expectedInfo.getBranch())) {
      logValueMismatchError("Branch", actualInfo.getBranch(), expectedInfo.getBranch());
      return false;
    }
    if (!Objects.equals(actualInfo.getPipeId(), expectedInfo.getPipeId())) {
      logValueMismatchError("Pipe Id", actualInfo.getPipeId(), expectedInfo.getPipeId());
      return false;
    }
    if (!Objects.equals(actualInfo.getPipeName(), expectedInfo.getPipeName())) {
      logValueMismatchError("Pipe name", actualInfo.getPipeName(), expectedInfo.getPipeName());
      return false;
    }
    if (!Objects.equals(
        actualInfo.getIngestionSourceType(), expectedInfo.getIngestionSourceType())) {
      logValueMismatchError(
          "Ingestion source type",
          actualInfo.getIngestionSourceType(),
          expectedInfo.getIngestionSourceType());
      return false;
    }
    if (!Objects.equals(actualInfo.getRequestId(), expectedInfo.getRequestId())) {
      logValueMismatchError("Request Id", actualInfo.getRequestId(), expectedInfo.getRequestId());
      return false;
    }
    if (actualInfo.getFileSize() != expectedInfo.getFileSize()) {
      logValueMismatchError("File size", actualInfo.getFileSize(), expectedInfo.getFileSize());
      return false;
    }
    if (!Objects.equals(
        actualInfo.getFileNotificationTimestamp(), expectedInfo.getFileNotificationTimestamp())) {
      logValueMismatchError(
          "File notification timestamp",
          actualInfo.getFileNotificationTimestamp(),
          expectedInfo.getFileNotificationTimestamp());
      return false;
    }
    if (actualInfo.getFirstErrorMessage() != null
        && !actualInfo.getFirstErrorMessage().startsWith(expectedInfo.getFirstErrorMessage())) {
      logValueMismatchError(
          "First error message",
          actualInfo.getFirstErrorMessage(),
          expectedInfo.getFirstErrorMessage());
      return false;
    }

    if (!Objects.equals(actualInfo.getFormatOptions(), expectedInfo.getFormatOptions())) {
      logValueMismatchError(
          "Format options", actualInfo.getFormatOptions(), expectedInfo.getFormatOptions());
      return false;
    }
    return true;
  }

  private void logValueMismatchError(String key, Object actual, Object expected) {
    LOGGER.error("{} does not match.\nActual: {}\nExpected: {}", key, actual, expected);
  }
}
