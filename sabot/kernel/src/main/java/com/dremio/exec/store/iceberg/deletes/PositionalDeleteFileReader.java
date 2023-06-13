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

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.MetadataColumns;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * A PositionalDeleteFileReader wraps a RecordReader for an Iceberg positional delete file, and exposes a sequence
 * of PositionalDeleteIterators for all or a subset of the data files tracked in the delete file.
 *
 * PositionalDeleteFileReader is reference counted.  The caller must set the refcount - either via the constructor
 * or by explicit calls to retain() - to be equal to the number of iterators it expects to create via calls to
 * createIteratorForDataFile().  The refcount is decremented when an iterator is closed.  When the refcount hits 0
 * the PositionalDeleteFileReader and child objects such as the underlying RecordReader will be closed.
 */
public class PositionalDeleteFileReader implements AutoCloseable {

  public static final String FILE_PATH_COLUMN = MetadataColumns.DELETE_FILE_PATH.name();
  public static final String POS_COLUMN = MetadataColumns.DELETE_FILE_POS.name();

  public static final BatchSchema SCHEMA = new BatchSchema(ImmutableList.of(
    Field.notNullable(FILE_PATH_COLUMN, new ArrowType.Utf8()),
    Field.notNullable(POS_COLUMN, new ArrowType.Int(64, true))
  ));

  private final VarCharVector pathVector;
  private final BigIntVector posVector;
  private final ArrowBuf currentDataFilePathBuf;
  private final NullableVarCharHolder holder;

  private RecordReader reader;
  private SampleMutator mutator;
  private String currentDataFilePath;
  private int currentDataFilePathEnd;
  private int current = -1;
  private int records = -1;
  private int activeIterators = 0;
  private int refCount;

  public PositionalDeleteFileReader(OperatorContext context, RecordReader reader, int initialRefCount) {
    Preconditions.checkNotNull(context);
    this.reader = Preconditions.checkNotNull(reader);
    this.mutator = createMutator(context.getAllocator());
    this.pathVector = (VarCharVector) mutator.getVector(FILE_PATH_COLUMN);
    this.posVector = (BigIntVector) mutator.getVector(POS_COLUMN);
    this.currentDataFilePathBuf = context.getManagedBuffer();
    this.holder = new NullableVarCharHolder();
    this.refCount = initialRefCount;
  }

  public void retain() {
    retain(1);
  }

  public void retain(int count) {
    refCount += count;
  }

  public void release() {
    Preconditions.checkState(refCount > 0);
    refCount--;
    if (refCount == 0) {
      AutoCloseables.close(RuntimeException.class, this);
    }
  }

  public int refCount() {
    return refCount;
  }

  public void setup() throws ExecutionSetupException {
    reader.setup(mutator);
  }

  public PositionalDeleteIterator createIteratorForDataFile(String dataFilePath) {
    Preconditions.checkState(activeIterators == 0, "Only one active iterator is allowed.");
    Preconditions.checkArgument(currentDataFilePath == null || currentDataFilePath.compareTo(dataFilePath) < 0,
        "Data file path iteration must be in ascending order for positional delete files.");
    advanceToDataFile(dataFilePath);
    activeIterators++;
    return new IteratorImpl();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(mutator, reader);
    reader = null;
    mutator = null;
  }

  private void getNextRecordBatch() {
    reader.allocate(mutator.getFieldVectorMap());
    records = reader.next();
    // check if the current batch has rows only for data files less than the current data file we're looking for,
    // and skip if so
    while (records > 0 && canSkipBatch()) {
      reader.allocate(mutator.getFieldVectorMap());
      records = reader.next();
    }
    current = 0;
  }

  private void advanceToDataFile(String dataFilePath) {
    currentDataFilePath = dataFilePath;
    byte[] bytes = currentDataFilePath.getBytes(StandardCharsets.UTF_8);
    currentDataFilePathBuf.setBytes(0, bytes);
    currentDataFilePathEnd = bytes.length;

    // skip ahead if current batch only has records for data files less than the current
    if (canSkipBatch()) {
      getNextRecordBatch();
    }

    // find the first matching row for the current data file in the current batch - if no match is found, we have
    // no records for the data file in this delete file
    int firstMatch = findFirstRecordInBatchForCurrentDataFile();
    if (firstMatch != -1) {
      current = firstMatch;
    }
  }

  private boolean canSkipBatch() {
    // check if last record in the batch has a path less than the current data file path
    return records == -1 || (records > 0 && comparePathAtIndex(records - 1) < 0);
  }

  private int findFirstRecordInBatchForCurrentDataFile() {
    // perform a binary search looking for the 1st record in the batch which matches the current data file path
    int left = current;
    int right = records - 1;
    int cmp = 0;
    while (left <= right) {
      int mid = (left + right) / 2;
      cmp = comparePathAtIndex(mid);
      if (cmp < 0) {
        left = mid + 1;
      } else if (cmp > 0) {
        right = mid - 1;
      } else if (left != mid) {
        right = mid;
      } else {
        return mid;
      }
    }

    return -1;
  }

  private int comparePathAtIndex(int index) {
    pathVector.get(index, holder);
    return ByteFunctionHelpers.compare(holder.buffer, holder.start, holder.end, currentDataFilePathBuf, 0,
        currentDataFilePathEnd);
  }

  private static SampleMutator createMutator(BufferAllocator allocator) {
    SampleMutator mutator = new SampleMutator(allocator);
    mutator.addField(SCHEMA.getColumn(0), VarCharVector.class);
    mutator.addField(SCHEMA.getColumn(1), BigIntVector.class);
    mutator.getContainer().buildSchema();
    return mutator;
  }

  /**
   * PositionalDeleteIterator implementation which will iterate over all the records for the current data file.
   * When done it will leave the reader at its current position so that a new data file iteration may start from
   * that point.
   */
  private class IteratorImpl implements PositionalDeleteIterator {

    private IteratorImpl() {
    }

    @Override
    public void close() throws Exception {
      Preconditions.checkState(activeIterators == 1, "Trying to close an already-closed iterator.");
      activeIterators--;
      release();
    }

    @Override
    public boolean hasNext() {
      if (records != 0 && current == records) {
        getNextRecordBatch();
      }
      return current < records && comparePathAtIndex(current) == 0;
    }

    @Override
    public Long next() {
      return posVector.get(current++);
    }
  }
}
