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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a set of varchar vectors. A varchar vector contains all the field
 * values for a given column. Each record is a single value within each vector of the set.
 */
class FieldVarCharOutput extends TextOutput {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FieldVarCharOutput.class);

  // array of output vector
  private final VarCharVector[] vectors;
  // boolean array indicating which fields are selected (if star query entire array is set to true)
  private final boolean[] selectedFields;
  // current vector to which field will be added
  private VarCharVector currentVector;
  // track which field is getting appended
  private int currentFieldIndex = -1;
  // track chars within field
  private int currentDataPointer = 0;
  // track if field is still getting appended
  private boolean fieldOpen = true;
  // holds chars for a field
  private final byte[] fieldBytes;

  private boolean collect = true;
  private boolean rowHasData = false;
  private int recordCount = 0;
  private int maxField = 0;

  /**
   * We initialize and add the varchar vector for each incoming field in this
   * constructor.
   * @param outputMutator  Used to create/modify schema
   * @param fieldNames Incoming field names
   * @param columns  List of columns selected in the query
   * @param isStarQuery  boolean to indicate if all fields are selected or not
   * @param sizeLimit Maximum size for an individual field
   * @throws SchemaChangeException
   */
  public FieldVarCharOutput(OutputMutator outputMutator, String[] fieldNames, Collection<SchemaPath> columns, boolean isStarQuery, int sizeLimit) throws SchemaChangeException {
    super(sizeLimit);

    int totalFields = fieldNames.length;
    List<String> outputColumns = new ArrayList<>(Arrays.asList(fieldNames));

    if (isStarQuery) {
      maxField = totalFields - 1;
      this.selectedFields = new boolean[totalFields];
      Arrays.fill(selectedFields, true);
    } else {
      List<Integer> columnIds = new ArrayList<Integer>();
      String pathStr;
      int index;

      for (SchemaPath path : columns) {
        pathStr = path.getRootSegment().getPath();
        index = getIndexOf(outputColumns, pathStr);
        if (index < 0) {
          // found col that is not a part of fieldNames, add it
          // this col might be part of some another scanner
          index = totalFields++;
          outputColumns.add(pathStr);
        }
        columnIds.add(index);
      }
      Collections.sort(columnIds);

      this.selectedFields = new boolean[totalFields];
      for(Integer i : columnIds) {
        selectedFields[i] = true;
        maxField = i;
      }
    }

    this.vectors = new VarCharVector[totalFields];

    for (int i = 0; i <= maxField; i++) {
      if (selectedFields[i]) {
        Field field = new Field(outputColumns.get(i), new FieldType(true, MinorType.VARCHAR.getType(), null), null);
        this.vectors[i] = outputMutator.addField(field, VarCharVector.class);
      }
    }

    this.fieldBytes = new byte[sizeLimit];
  }

  /**
   * Start a new record batch. Resets all pointers
   */
  @Override
  public void startBatch() {
    this.recordCount = 0;
    this.currentFieldIndex = -1;
    this.collect = true;
    this.fieldOpen = false;
  }

  private int getIndexOf(List<String> outputColumns, String pathStr) {
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
    fieldOpen = true;
    collect = selectedFields[index];
    currentVector = vectors[index];
  }

  @Override
  public void append(byte data) {
    if (!collect) {
      return;
    }

    FieldSizeLimitExceptionHelper.checkSizeLimit(currentDataPointer, maxCellLimit, currentFieldIndex, logger);
    fieldBytes[currentDataPointer++] = data;
  }

  @Override
  public boolean endField() {
    fieldOpen = false;
    FieldSizeLimitExceptionHelper.checkSizeLimit(currentDataPointer, maxCellLimit, currentFieldIndex, logger);

    if(collect) {
      assert currentVector != null;
      currentVector.setSafe(recordCount, fieldBytes, 0, currentDataPointer);
    }

    if (currentDataPointer > 0) {
      this.rowHasData = true;
    }

    return currentFieldIndex < maxField;
  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

 @Override
  public void finishRecord() {
    if(fieldOpen){
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

 }
