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
package com.dremio.service.jobs;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.util.Pair;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.DremioGetObject;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.service.job.proto.JobId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

/**
 * Part of job results
 */
public class JobDataFragmentImpl implements JobDataFragment {
  private final JobId jobId;
  private final RecordBatches recordBatches;

  // transient map to avoid constantly searching linear list.
  private final Map<String, Integer> nameToColumnIndex;

  public JobDataFragmentImpl(final RecordBatches recordBatches, final int offsetInJobResults, final JobId jobId) {
    this.recordBatches = recordBatches;
    this.jobId = jobId;
    this.nameToColumnIndex = getColumnIndicesFromSchema(recordBatches.getSchema());
  }

  @Override
  public BatchSchema getSchema() {
    return recordBatches.getSchema();
  }

  @Override
  public List<RecordBatchHolder> getRecordBatches() {
    return recordBatches.getBatches();
  }

  @Override
  public int getReturnedRowCount() {
    return recordBatches.getSize();
  }

  @Override
  public JobId getJobId() {
    return jobId;
  }

  @Override
  public Object extractValue(String column, int index){
    Pair<RecordBatchData, Integer> dataBatch = find(index);
    Integer columnIndex = nameToColumnIndex.get(column);
    return DremioGetObject.getObject(dataBatch.getKey().getVectors().get(columnIndex), dataBatch.getValue());
  }

  private Pair<RecordBatchData, Integer> find(int index) {
    if (index >= recordBatches.getSize()) {
      throw new IllegalArgumentException(String.format("Invalid index %s", index));
    }

    // Add the offset in the first batch
    int indexWorkspace = index;
    for(RecordBatchHolder batchHolder : recordBatches.getBatches()) {
      if (indexWorkspace < batchHolder.size()) {
        return new Pair<>(batchHolder.getData(), batchHolder.getStart() + indexWorkspace);
      }
      indexWorkspace -= batchHolder.size();
    }
    throw new IllegalArgumentException(String.format("Invalid index %s", index));
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized void close(){
    try{
      AutoCloseables.close( (List<AutoCloseable>) (Object) recordBatches.getBatches());
    }catch(Exception ex){
      Throwables.propagate(ex);
    }
  }

  private static Map<String, Integer> getColumnIndicesFromSchema(BatchSchema schema){
    ImmutableMap.Builder<String, Integer> columns = ImmutableMap.builder();

    for (int i = 0; i < schema.getFieldCount(); ++i) {
      final Field column = schema.getColumn(i);
      final String name = column.getName();

      columns.put(name, i);
    }

    return columns.build();
  }
}
