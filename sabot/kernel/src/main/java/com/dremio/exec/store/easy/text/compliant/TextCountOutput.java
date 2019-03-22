/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.easy.text.compliant;


import org.apache.arrow.flatbuf.RecordBatch;

import com.dremio.exec.exception.SchemaChangeException;

/**
 * Simple extension of {@link TextOutput}, to just count the number of records in text file. It outputs column called
 * "count" of boolean type and for each row the value is "true".
 */
public class TextCountOutput extends TextOutput  {

  private int recordCount = 0; /** int should be enough a batch can have {@link RecordBatch#MAX_BATCH_SIZE} */
  private boolean fieldOpen = false;

  public TextCountOutput() throws SchemaChangeException {
    super(1); // Default cell size, which is ignored for this output so use a dummy value.
  }

  @Override
  public void startField(int index) {
    fieldOpen = true;
  }

  @Override
  public boolean endField() {
    fieldOpen = false;
    // We just need to process one filed in each record.
    return false;
  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

  @Override
  public void appendIgnoringWhitespace(byte data) {
    // no-op
  }

  @Override
  public void append(byte data) {
    // no-op
  }

  @Override
  public void finishRecord() {
    if (fieldOpen) {
      endField();
    }
    recordCount++;
  }

  @Override
  public long getRecordCount() {
    return recordCount;
  }

  @Override
  public void startBatch() {
    recordCount = 0;
  }

  @Override
  public void finishBatch() {
  }

  @Override
  public boolean rowHasData() {
    return true;
  }
}
