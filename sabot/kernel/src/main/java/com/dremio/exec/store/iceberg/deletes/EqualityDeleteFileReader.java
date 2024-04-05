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
package com.dremio.exec.store.iceberg.deletes;

import static com.dremio.sabot.op.common.ht2.LBlockHashTable.ORDINAL_SIZE;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

/**
 * This class allows for creation of an {@link EqualityDeleteHashTable} from a {@link RecordReader}
 * instance.
 */
@NotThreadSafe
public class EqualityDeleteFileReader implements AutoCloseable {

  private final OperatorContext context;
  private final List<SchemaPath> equalityFields;
  private final int tableSize;

  private RecordReader reader;
  private SampleMutator mutator;

  public EqualityDeleteFileReader(
      OperatorContext context,
      RecordReader reader,
      BatchSchema schema,
      List<SchemaPath> equalityFields,
      long recordCount) {
    Preconditions.checkArgument(
        recordCount < Integer.MAX_VALUE,
        String.format(
            "Equality delete files with greater than %d records are not supported. File: %s, record count: %d",
            Integer.MAX_VALUE - 1, reader.getFilePath(), recordCount));
    this.context = Preconditions.checkNotNull(context);
    this.equalityFields = Preconditions.checkNotNull(equalityFields);
    this.reader = Preconditions.checkNotNull(reader);
    this.mutator = createMutator(context.getAllocator(), schema, equalityFields);
    this.tableSize = (int) recordCount;
  }

  public void setup() throws ExecutionSetupException {
    reader.setup(mutator);
  }

  public List<SchemaPath> getEqualityFields() {
    return equalityFields;
  }

  public EqualityDeleteHashTable buildHashTable() {
    try {
      reader.allocate(mutator.getFieldVectorMap());

      List<FieldVector> equalityVectors =
          mutator.getVectors().stream().map(v -> (FieldVector) v).collect(Collectors.toList());

      try (EqualityDeleteHashTable.Builder builder =
              new EqualityDeleteHashTable.Builder(
                  context.getAllocator(),
                  equalityFields,
                  equalityVectors,
                  tableSize,
                  context.getTargetBatchSize());
          ArrowBuf outOrdinals =
              context.getAllocator().buffer((long) context.getTargetBatchSize() * ORDINAL_SIZE)) {

        int records;
        while ((records = reader.next()) > 0) {
          builder.insertBatch(records, outOrdinals);
          reader.allocate(mutator.getFieldVectorMap());
        }

        return builder.build();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(mutator, reader);
    reader = null;
    mutator = null;
  }

  private static SampleMutator createMutator(
      BufferAllocator allocator, BatchSchema schema, List<SchemaPath> equalityFields) {
    SampleMutator mutator = new SampleMutator(allocator);
    schema.materializeVectors(equalityFields, mutator);
    mutator.getContainer().buildSchema();
    return mutator;
  }
}
