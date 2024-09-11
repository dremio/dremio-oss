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
package com.dremio.exec.store.easy.text.compliant;

import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.exec.exception.SchemaChangeException;
import java.util.List;
import org.apache.arrow.vector.ValueVector;

/** Abstract Class, responsible for generating record batches for text file inputs. */
abstract class FieldTypeOutput extends TextOutput {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldTypeOutput.class);

  // array of output vector
  protected final ValueVector[] vectors;
  // boolean array indicating which fields are selected (if star query entire array is set to true)
  protected final boolean[] selectedFields;
  // current vector to which field will be added
  protected ValueVector currentVector;
  // track which field is getting appended
  protected int currentFieldIndex = -1;
  // track chars within field
  private int currentDataPointer = 0;
  // track if field is still getting appended
  protected boolean fieldOpen = false;

  protected boolean collect = true;
  protected boolean rowHasData = false;
  int recordCount = 0;
  protected int maxField = 0;
  protected boolean isValidationMode = false;

  /**
   * We initialize and add the varchar vector for each incoming field in this constructor.
   *
   * @param sizeLimit Maximum size for an individual field
   * @throws SchemaChangeException
   */
  public FieldTypeOutput(int sizeLimit, int totalFields) throws SchemaChangeException {
    super(sizeLimit);
    this.selectedFields = new boolean[totalFields];
    this.vectors = new ValueVector[totalFields];
  }

  /** Start a new record batch. Resets all pointers */
  @Override
  public void startBatch() {
    this.recordCount = 0;
    this.currentFieldIndex = -1;
    this.collect = true;
    this.fieldOpen = false;
  }

  protected int getIndexOf(List<String> outputColumns, String pathStr) {
    for (int i = 0; i < outputColumns.size(); i++) {
      if (outputColumns.get(i).equalsIgnoreCase(pathStr)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public void startField(int index) {
    currentFieldIndex = index;
    currentDataPointer = 0;
    rowHasData = false;
    fieldOpen = true;
    collect = selectedFields[index];
    currentVector = vectors[index];
  }

  @Override
  public void append(byte data) {
    if (!collect) {
      return;
    }

    FieldSizeLimitExceptionHelper.checkSizeLimit(
        currentDataPointer + 1, maxCellLimit, currentFieldIndex, logger);
    appendByte(currentDataPointer, data);
    currentDataPointer++;
    rowHasData = true;
  }

  @Override
  public boolean endField() {
    fieldOpen = false;
    FieldSizeLimitExceptionHelper.checkSizeLimit(
        currentDataPointer, maxCellLimit, currentFieldIndex, logger);

    if (collect) {
      assert isValidationMode || currentVector != null;
      writeValueInCurrentVector(recordCount, currentDataPointer);
    }

    return currentFieldIndex < maxField;
  }

  protected abstract void appendByte(int currentIndex, byte b);

  protected abstract void writeValueInCurrentVector(int index, int endIndex);

  @Override
  public boolean endEmptyField() {
    return endField();
  }

  @Override
  public void finishRecord() {
    if (fieldOpen) {
      endField();
    }

    recordCount++;
  }

  // Sets the record count in this batch within the value vector
  @Override
  public void finishBatch() {

    for (int i = 0; i <= maxField; i++) {
      if (this.vectors[i] != null) {
        this.vectors[i].setValueCount(recordCount);
      }
    }
  }

  @Override
  public long getRecordCount() {
    return recordCount;
  }

  @Override
  public boolean rowHasData() {
    return this.rowHasData;
  }

  @Override
  public int getFieldCurrentDataPointer() {
    return currentDataPointer;
  }

  @Override
  public void setFieldCurrentDataPointer(int currentDataPointer) {
    this.currentDataPointer = currentDataPointer;
  }
}
