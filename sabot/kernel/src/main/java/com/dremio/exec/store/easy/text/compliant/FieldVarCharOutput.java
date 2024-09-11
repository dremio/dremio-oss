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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.sabot.op.scan.OutputMutator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Class is responsible for generating record batches for text file inputs. We generate a record
 * batch with a set of varchar vectors. A varchar vector contains all the field values for a given
 * column. Each record is a single value within each vector of the set.
 */
class FieldVarCharOutput extends FieldTypeOutput {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FieldVarCharOutput.class);

  // holds data as the column is being read
  private ArrowBuf tmpBuf = null;
  private final OutputMutator outputMutator;

  /**
   * We initialize and add the varchar vector for each incoming field in this constructor.
   *
   * @param outputMutator Used to create/modify schema
   * @param fieldNames Incoming field names
   * @param columns List of columns selected in the query
   * @param isStarQuery boolean to indicate if all fields are selected or not
   * @param sizeLimit Maximum size for an individual field
   * @throws SchemaChangeException
   */
  public FieldVarCharOutput(
      OutputMutator outputMutator,
      String[] fieldNames,
      Collection<SchemaPath> columns,
      boolean isStarQuery,
      int sizeLimit)
      throws SchemaChangeException {
    super(sizeLimit, (fieldNames.length + columns.size()));

    int totalFields = fieldNames.length;
    List<String> outputColumns = new ArrayList<>(Arrays.asList(fieldNames));

    if (isStarQuery) {
      maxField = totalFields - 1;
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

      for (Integer i : columnIds) {
        selectedFields[i] = true;
        maxField = i;
      }
    }

    this.outputMutator = outputMutator;
    for (int i = 0; i <= maxField; i++) {
      if (selectedFields[i]) {
        Field field =
            new Field(
                outputColumns.get(i), new FieldType(true, MinorType.VARCHAR.getType(), null), null);
        this.vectors[i] = outputMutator.addField(field, VarCharVector.class);
      }
    }
  }

  @Override
  public void init() {
    this.tmpBuf = this.outputMutator.getManagedBuffer();
  }

  @Override
  public void close() {
    if (this.tmpBuf != null) {
      this.tmpBuf.clear();
    }
  }

  private void expandTmpBufIfNecessary(int currentIndex) {
    if (currentIndex < tmpBuf.capacity()) {
      return;
    }

    ArrowBuf oldBuf = tmpBuf;
    // addref
    oldBuf.getReferenceManager().retain();
    try {
      tmpBuf = tmpBuf.reallocIfNeeded(Math.min(tmpBuf.capacity() * 2, maxCellLimit + 1));
      tmpBuf.setBytes(0, oldBuf, 0, oldBuf.capacity());
    } finally {
      oldBuf.getReferenceManager().release();
    }
  }

  @Override
  protected void appendByte(int currentIndex, byte b) {
    // copy b into tmpBuf
    // size limit check has already been done
    expandTmpBufIfNecessary(currentIndex);
    tmpBuf.setByte(currentIndex, b);
  }

  @Override
  protected void writeValueInCurrentVector(int index, int endIndex) {
    ((VarCharVector) currentVector).setSafe(index, 0, endIndex, tmpBuf);
    tmpBuf.clear();
  }
}
