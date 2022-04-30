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
package com.dremio.exec.store.mock;

import static com.dremio.common.util.MajorTypeHelper.getArrowMinorType;
import static com.dremio.common.util.MajorTypeHelper.getArrowTypeForMajorType;
import static org.apache.arrow.vector.types.Types.getMinorTypeForArrowType;

import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.GenerateSampleData;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.mock.MockGroupScanPOP.MockColumn;
import com.dremio.exec.store.mock.MockGroupScanPOP.MockScanEntry;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;

public class MockRecordReader extends AbstractRecordReader {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockRecordReader.class);

  private final MockScanEntry config;
  private final OperatorContext context;
  private ValueVector[] valueVectors;
  private int recordsRead;
  private int batchRecordCount;


  public MockRecordReader(OperatorContext context, MockScanEntry config) {
    super(context, null);
    this.context = context;
    this.config = config;
  }

  private int getEstimatedRecordSize(MockColumn[] types) {
    int x = 0;
    for (int i = 0; i < types.length; i++) {
      x += TypeHelper.getSize(getArrowMinorType(types[i].getMajorType().getMinorType()));
    }
    return x;
  }

  private Field getVector(String name, MajorType type, int length) {
    assert context != null : "Context shouldn't be null.";
    final Field f = new Field(name, new FieldType(true, getArrowTypeForMajorType(type), null), null);
    return f;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      final int estimateRowSize = getEstimatedRecordSize(config.getTypes());
      valueVectors = new ValueVector[config.getTypes().length];
      batchRecordCount = 250000 / estimateRowSize;

      for (int i = 0; i < config.getTypes().length; i++) {
        final MajorType type = config.getTypes()[i].getMajorType();
        final Field field = getVector(config.getTypes()[i].getName(), type, batchRecordCount);
        final Class<? extends ValueVector> vvClass = (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(getMinorTypeForArrowType(field.getType()));
        valueVectors[i] = output.addField(field, vvClass);
      }
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }
  }

  @Override
  public int next() {
    if (recordsRead >= this.config.getRecords()) {
      return 0;
    }

    final int recordSetSize = Math.min(batchRecordCount, this.config.getRecords() - recordsRead);
    recordsRead += recordSetSize;
    for (final ValueVector v : valueVectors) {
      GenerateSampleData.generateTestData(v, recordSetSize);
    }

    return recordSetSize;
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public void close() {
  }
}
