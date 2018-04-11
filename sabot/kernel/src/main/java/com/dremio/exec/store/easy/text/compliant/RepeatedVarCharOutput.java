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

import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a single vector of type repeated varchar vector. Each record is a single
 * value within the vector containing all the fields in the record as individual array elements.
 */
class RepeatedVarCharOutput extends TextOutput {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RepeatedVarCharOutput.class);

  static final String COL_NAME = "columns";
  static final SchemaPath COLUMNS = SchemaPath.getSimplePath("columns");
  public static final int MAXIMUM_NUMBER_COLUMNS = 64 * 1024;

  private final OutputMutator output;

  private final VectorContainerWriter rootWriter;

  private final ListWriter listWriter;

  private ArrowBuf tmpBuf;

  // boolean array indicating which fields are selected (if star query entire array is set to true)
  private final boolean[] collectedFields;

  private boolean hasData;

  // total number of records processed (across batches)
  private long recordCount;

  // number of records processed in this current batch
  private int batchIndex;

  // current index of the field being processed within the record
  private int fieldIndex = -1;

  /* boolean to indicate if we are currently appending data to the output vector
   * Its set to false when we have hit out of memory or we are not interested in
   * the particular field
   */
  private boolean collect;

  // are we currently appending to a field
  private boolean fieldOpen;

  // maximum number of fields/columns
  private final int maxField;

  private int charLengthOffset;

  /**
   * We initialize and add the repeated varchar vector to the record batch in this
   * constructor. Perform some sanity checks if the selected columns are valid or not.
   * @param outputMutator  Used to create/modify schema in the record batch
   * @param columns  List of columns selected in the query
   * @param isStarQuery  boolean to indicate if all fields are selected or not
   * @throws SchemaChangeException
   */
  public RepeatedVarCharOutput(OutputMutator outputMutator, Collection<SchemaPath> columns, boolean isStarQuery) throws SchemaChangeException {
    super();

    this.output = outputMutator;
    rootWriter = new VectorContainerWriter(outputMutator);
    listWriter = rootWriter.rootAsMap().list(COL_NAME);
    tmpBuf = outputMutator.getManagedBuffer();

    { // setup fields
      List<Integer> columnIds = new ArrayList<>();
      if (!isStarQuery) {
        String pathStr;
        for (SchemaPath path : columns) {
          assert path.getRootSegment().isNamed() : "root segment should be named";
          pathStr = path.getRootSegment().getPath();
          Preconditions.checkArgument(pathStr.equals(COL_NAME) || (pathStr.equals("*") && path.getRootSegment().getChild() == null),
              String.format("Selected column '%s' must have name 'columns' or must be plain '*'", pathStr));

          if (path.getRootSegment().getChild() != null) {
            Preconditions.checkArgument(path.getRootSegment().getChild().isArray(),
              String.format("Selected column '%s' must be an array index", pathStr));
            int index = path.getRootSegment().getChild().getArraySegment().getIndex();
            columnIds.add(index);
          }
        }
        Collections.sort(columnIds);

      }

      boolean[] fields = new boolean[MAXIMUM_NUMBER_COLUMNS];

      int maxField = fields.length;

      if(isStarQuery){
        Arrays.fill(fields, true);
      }else{
        for(Integer i : columnIds){
          maxField = 0;
          maxField = Math.max(maxField, i);
          fields[i] = true;
        }
      }
      this.collectedFields = fields;
      this.maxField = maxField;
    }


  }

  /**
   * Start a new record batch. Resets all the offsets and pointers that
   * store buffer addresses
   */
  @Override
  public void startBatch() {
    this.fieldOpen = false;
    this.batchIndex = 0;
    this.fieldIndex = -1;
    this.collect = true;
  }

  /**
   * Helper method to check if the buffer we are accessing
   * has a minimum reference count and has not been deallocated
   * @param b  working Arrow buffer
   */
  private void checkBuf(ArrowBuf b){
    if(b.refCnt() < 1){
      throw new IllegalStateException("Cannot access a dereferenced buffer.");
    }
  }

  private void expandTmpBufIfNecessary() {
    if (charLengthOffset < tmpBuf.capacity()) {
      return;
    }
    byte[] tmp = new byte[tmpBuf.capacity()];
    tmpBuf.getBytes(0, tmp);
    tmpBuf = tmpBuf.reallocIfNeeded(tmpBuf.capacity() * 2);
    tmpBuf.setBytes(0, tmp);
    charLengthOffset = tmp.length;
  }

  @Override
  public void startField(int index) {
    fieldIndex = index;
    collect = collectedFields[index];
    fieldOpen = true;
    if (!hasData) {
      rootWriter.setPosition(batchIndex);
      listWriter.startList();
      hasData = true;
    }
  }

  @Override
  public boolean endField() {
    fieldOpen = false;
    listWriter.varChar().writeVarChar(0, charLengthOffset, tmpBuf);
    charLengthOffset = 0;
    return true;
  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

  @Override
  public void append(byte data) {
    if(!collect){
      return;
    }
    expandTmpBufIfNecessary();
    tmpBuf.setByte(charLengthOffset, data);
    charLengthOffset++;
  }

  @Override
  public long getRecordCount() {
    return recordCount;
  }

  @Override
  public boolean rowHasData() {
    return hasData;
  }

  @Override
  public void finishRecord() {
    hasData = false;

    if(fieldOpen){
      endField();
    }

    listWriter.endList();

    // if there were no defined fields, skip.
//    if(fieldIndex > -1){
      batchIndex++;
      recordCount++;
//    }


  }

  @Override
  public void close() {
    tmpBuf.clear();
  }

  /**
   * This method is a helper method added for DRILL-951
   * TextRecordReader to call this method to get field names out
   * @return array of field data strings
   */
  public String [] getTextOutput () throws ExecutionSetupException {
    if (recordCount == 0 || fieldIndex == -1) {
      return null;
    }

    //Currently only first line header is supported. Return only first record.
    int retSize = fieldIndex+1;
    String [] out = new String [retSize];

    try {
      ListVector listVector = output.addField(new Field(COL_NAME, true, MinorType.LIST.getType(), null), ListVector.class);
      List outputlist = (List) listVector.getObject((int)(recordCount-1));

      for (int i=0; i<retSize; i++){
        out[i] = ((Text) outputlist.get(i)).toString();
      }
      return out;
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  // Sets the record count in this batch within the value vector
  @Override
  public void finishBatch() {
    if (fieldOpen) {
      endField();
    }
    if (hasData) {
      finishRecord();
    }
    rootWriter.setValueCount(batchIndex);
  }
}
