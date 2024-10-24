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
package com.dremio.sabot;

import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;

/**
 * Generates data of a single data type for operator-level unit tests. Input - FieldInfo (to know
 * what type of vectors to generate), # of batches, total # of rows, batch size, function to
 * generate random values for the data, input vector Output - in next function, next batch of data
 * will be placed in the input vector
 *
 * @param <T>
 */
public class TypeDataGenerator<T extends Comparable<T>> implements Generator {
  private final FieldVector vector;
  List<T> dataList = new ArrayList<>();
  private int absoluteIndex;
  private final int numOfBatches;
  private final RandomGenerator<T> randomGenerator;
  int numOfRows;
  private final FieldInfo fieldInfo;
  private final int batchSize;
  private VectorContainer container;

  public TypeDataGenerator(
      int numOfBatches,
      int numOfRows,
      RandomGenerator<T> randomGenerator,
      FieldInfo fieldInfo,
      int batchSize,
      FieldVector vector) {
    this.numOfBatches = numOfBatches;
    this.randomGenerator = randomGenerator;
    this.numOfRows = numOfRows;
    this.fieldInfo = fieldInfo;
    this.batchSize = batchSize;
    this.vector = vector;
  }

  public void generateValues() {
    int numOfUniqueKeysInBatch = Math.min(fieldInfo.getNumOfUniqueValuesInBatch(), batchSize);
    dataList = new ArrayList<>();
    int groupInterval = batchSize / numOfUniqueKeysInBatch;
    int batchIndex = 0;
    T currentValue = randomGenerator.getRandomValue();

    for (int i = 0; i < batchSize; i++) {
      dataList.add(currentValue);
      if ((i + 1) % groupInterval == 0) {
        currentValue = randomGenerator.getRandomValue();
      }
    }
    switch (fieldInfo.getSortOrder()) {
      case RANDOM:
        Collections.shuffle(dataList);
        break;
      case ASCENDING:
        Collections.sort(dataList);
        break;
      case DESCENDING:
        Collections.sort(dataList);
        Collections.reverse(dataList);
        break;
    }
  }

  @Override
  public VectorAccessible getOutput() {
    if (container == null) {
      container = new VectorContainer(vector.getAllocator());
      container.add(vector);
    }
    return container;
  }

  @Override
  public int next(int records) {
    generateValues();
    if (absoluteIndex >= numOfRows) {
      return 0;
    }
    switch (fieldInfo.getField().getFieldType().getType().getTypeID()) {
      case Int:
        for (int i = 0; i < records && absoluteIndex < numOfRows; i++, absoluteIndex++) {
          ((IntVector) vector).setSafe(i, (Integer) dataList.get(i));
        }
        break;
      case Utf8:
        for (int i = 0; i < records && absoluteIndex < numOfRows; i++, absoluteIndex++) {
          ((BaseVariableWidthVector) vector).setSafe(i, ((String) dataList.get(i)).getBytes());
        }
        break;
      case Date:
        for (int i = 0; i < records && absoluteIndex < numOfRows; i++, absoluteIndex++) {
          Date d = (Date) dataList.get(i);
          ((DateMilliVector) vector).setSafe(i, d.toInstant().toEpochMilli());
        }
        break;
      case Bool:
        for (int i = 0; i < records && absoluteIndex < numOfRows; i++, absoluteIndex++) {
          ((BitVector) vector).setSafe(i, (Boolean) dataList.get(i) ? 1 : 0);
        }
        break;
    }
    return records;
  }

  @Override
  public void close() throws Exception {
    if (this.container != null) {
      container.close();
    }
  }

  @FunctionalInterface
  public interface RandomGenerator<T> {
    T getRandomValue();
  }
}
