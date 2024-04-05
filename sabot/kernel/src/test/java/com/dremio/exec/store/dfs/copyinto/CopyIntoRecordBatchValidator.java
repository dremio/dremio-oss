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
import de.vandermeer.asciitable.v2.V2_AsciiTable;
import java.util.List;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.reader.FieldReader;

public class CopyIntoRecordBatchValidator extends RecordBatchValidatorDefaultImpl {

  public CopyIntoRecordBatchValidator(RecordSet recordSet) {
    super(recordSet);
  }

  @Override
  protected boolean compareRecord(
      RecordSet.Record expected, List<ValueVector> actual, int actualIndex, V2_AsciiTable output) {
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
      return false;
    }
    if (actualInfo.getRecordsLoadedCount() != expectedInfo.getRecordsLoadedCount()) {
      return false;
    }
    if (!actualInfo.getTableName().equals(expectedInfo.getTableName())) {
      return false;
    }
    if (actualInfo.getFilePath() == null || actualInfo.getFilePath().isEmpty()) {
      return false;
    }
    if (!actualInfo.getQueryUser().equals(expectedInfo.getQueryUser())) {
      return false;
    }
    if (!actualInfo.getQueryId().equals(expectedInfo.getQueryId())) {
      return false;
    }
    if (!actualInfo.getFileState().equals(expectedInfo.getFileState())) {
      return false;
    }
    return actualInfo.getFormatOptions().equals(expectedInfo.getFormatOptions());
  }
}
