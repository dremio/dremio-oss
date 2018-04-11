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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
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
  static final String COL_NAME = "columns";

  // array of output vector
  private final NullableVarCharVector[] vectors;
  // boolean array indicating which fields are selected (if star query entire array is set to true)
  private final boolean[] selectedFields;
  // current vector to which field will be added
  private NullableVarCharVector currentVector;
  // track which field is getting appended
  private int currentFieldIndex = -1;
  // track chars within field
  private int currentDataPointer = 0;
  // track if field is still getting appended
  private boolean fieldOpen = true;
  // holds chars for a field
  private byte[] fieldBytes;

  private boolean collect = true;
  private boolean rowHasData= false;
  private static final int MAX_FIELD_LENGTH = 1024 * 64;
  private int recordCount = 0;
  private int maxField = 0;

  /**
   * We initialize and add the varchar vector for each incoming field in this
   * constructor.
   * @param outputMutator  Used to create/modify schema
   * @param fieldNames Incoming field names
   * @param columns  List of columns selected in the query
   * @param isStarQuery  boolean to indicate if all fields are selected or not
   * @throws SchemaChangeException
   */
  public FieldVarCharOutput(OutputMutator outputMutator, String [] fieldNames, Collection<SchemaPath> columns, boolean isStarQuery) throws SchemaChangeException {

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
        index = outputColumns.indexOf(pathStr);
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

    this.vectors = new NullableVarCharVector[totalFields];

    for (int i = 0; i <= maxField; i++) {
      if (selectedFields[i]) {
        Field field = new Field(outputColumns.get(i), true, MinorType.VARCHAR.getType(), null);
        this.vectors[i] = outputMutator.addField(field, NullableVarCharVector.class);
      }
    }

    this.fieldBytes = new byte[MAX_FIELD_LENGTH];

  }

  /**
   * Start a new record batch. Resets all pointers
   */
  @Override
  public void startBatch() {
    this.recordCount = 0;
    this.currentFieldIndex= -1;
    this.collect = true;
    this.fieldOpen = false;
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

    if (currentDataPointer >= MAX_FIELD_LENGTH -1) {
      throw UserException
          .unsupportedError()
          .message("Trying to write something big in a column")
          .addContext("columnIndex", currentFieldIndex)
          .addContext("Limit", MAX_FIELD_LENGTH)
          .build(logger);
    }

    fieldBytes[currentDataPointer++] = data;
  }

  @Override
  public boolean endField() {
    fieldOpen = false;

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
