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
package com.dremio.exec.store;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

public abstract class AbstractRecordReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractRecordReader.class);

  private static final String COL_NULL_ERROR = "Columns cannot be null. Use star column to select all fields.";
  public static final SchemaPath STAR_COLUMN = SchemaPath.getSimplePath("*");

  private Collection<SchemaPath> columns = null;
  private boolean isStarQuery = false;
  private boolean isSkipQuery = false;
  protected int numRowsPerBatch;
  protected long numBytesPerBatch;
  protected final OperatorContext context;

  public AbstractRecordReader(final OperatorContext context, final List<SchemaPath> columns) {
    this.context = context;
    if (context == null) {
      this.numRowsPerBatch = Ints.saturatedCast(ExecConstants.TARGET_BATCH_RECORDS_MAX.getDefault().getNumVal());
    } else {
      this.numRowsPerBatch = context.getTargetBatchSize();
    }

    if (context == null
      || context.getOptions() == null
      || context.getOptions().getOption(ExecConstants.OPERATOR_TARGET_BATCH_BYTES) == null) {
      this.numBytesPerBatch = ExecConstants.OPERATOR_TARGET_BATCH_BYTES_VALIDATOR.getDefault().getNumVal();
    } else {
      this.numBytesPerBatch = context.getOptions().getOption(ExecConstants.OPERATOR_TARGET_BATCH_BYTES).getNumVal();
    }

    if (columns != null) {
      setColumns(columns);
    }
  }

  public long getNumRowsPerBatch() {
    return numRowsPerBatch;
  }

  @Override
  public String toString() {
    return super.toString()
        + "[columns = " + columns
        + ", isStarQuery = " + isStarQuery
        + ", isSkipQuery = " + isSkipQuery + "]";
  }

  /**
   *
   * @param projected : The column list to be returned from this RecordReader.
   *                  1) empty column list: this is for skipAll query. It's up to each storage-plugin to
   *                  choose different policy of handling skipAll query. By default, it will use * column.
   *                  2) NULL : is NOT allowed. It requires the planner's rule, or GroupScan or ScanBatchCreator to handle NULL.
   */
  private void setColumns(Collection<SchemaPath> projected) {
    Preconditions.checkNotNull(projected, COL_NULL_ERROR);
    isSkipQuery = projected.isEmpty();
    Collection<SchemaPath> columnsToRead = projected;

    // If no column is required (SkipQuery), by default it will use DEFAULT_COLS_TO_READ .
    // Handling SkipQuery is storage-plugin specific : JSON, text reader, parquet will override, in order to
    // improve query performance.
    if (projected.isEmpty()) {
      if (supportsSkipAllQuery()) {
        columnsToRead = Collections.emptyList();
      } else {
        columnsToRead = GroupScan.ALL_COLUMNS;
      }
    }

    isStarQuery = ColumnUtils.isStarQuery(columnsToRead);
    columns = transformColumns(columnsToRead);

    logger.debug("columns to read : {}", columns);
  }

  protected boolean supportsSkipAllQuery(){
    return false;
  }

  protected Collection<SchemaPath> getColumns() {
    return columns;
  }

  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projected) {
    return projected;
  }

  protected boolean isStarQuery() {
    return isStarQuery;
  }

  /**
   * Returns true if reader should skip all of the columns, reporting number of records only. Handling of a skip query
   * is storage plugin-specific.
   */
  protected boolean isSkipQuery() {
    return isSkipQuery;
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  /**
   * Returns a message to be shown to the user if an exception is thrown that can't be processed
   * properly because its cause is not recognizable, typically because we can't have a dependency
   * on its type.
   * The message is a best-effort one.
   * @param t Throwable presented as the cause of the IOException.
   * @return a String to be shown to the user, or null if there is no reasonable response.
   */
  @Nullable
  public String bestEffortMessageForUnknownException(Throwable t) {
    if (t == null) {
      return null;
    }
    String tString = t.toString();
    // For exceptions involving AmazonS3Exception (DX-21818):
    if (tString.contains("AmazonS3Exception")) {
      if (tString.contains("Requests specifying Server Side Encryption with "
        + "AWS KMS managed keys must be made over a secure connection.")) {
        return "The request failed because it was made over an insecure connection. "
          + "Check the 'Encrypt Connection' box when creating the source.";
      } else {
        return "The request failed with the following cause: " + tString;
      }
    } else {
      return null;
    }
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    logger.debug("Dropping runtime filter from {} because the reader does not support runtime filtering", runtimeFilter.getSenderInfo());
    context.getStats().addLongStat(ScanOperator.Metric.RUNTIME_COL_FILTER_DROP_COUNT, runtimeFilter.getNonPartitionColumnFilters().size());
  }
}
