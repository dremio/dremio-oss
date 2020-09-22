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
package com.dremio.dac.model.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobDataFragmentImpl;
import com.dremio.service.jobs.RecordBatches;

/**
 * JobDataFragment that releases/closes itself after serialization.
 */
public class ReleasingData extends JobDataFragmentImpl implements ReleaseAfterSerialization {

  private final AutoCloseables.RollbackCloseable closeable;

  public ReleasingData(AutoCloseables.RollbackCloseable closeable, RBDList rbdList) {
    this(closeable, rbdList.toRecordBatches(), new JobId().setId("preview-job").setName("preview-job"));
  }

  private ReleasingData(AutoCloseables.RollbackCloseable closeable, RecordBatches recordBatches, JobId jobId) {
    super(recordBatches, 0, jobId);
    this.closeable = closeable;
  }

  public static ReleasingData from(RecordBatches recordBatches, JobId jobId) {
    return new ReleasingData(null, recordBatches, jobId);
  }

  @Override
  public synchronized void close() {
    List<AutoCloseable> closeables = new ArrayList<>();
    closeables.add(() -> super.close());
    List<AutoCloseable> backList = new ArrayList<>();
    if (closeable != null) {
      backList.addAll(closeable.getCloseables());
    }
    Collections.reverse(backList);
    closeables.addAll(backList);
    AutoCloseables.closeNoChecked(AutoCloseables.all(closeables));
  }

  /**
   * RecordBatchData List
   */
  public static class RBDList implements AutoCloseable {

    private final List<RecordBatchData> batches = new ArrayList<>();
    private final VectorAccessible accessible;
    private final BufferAllocator allocator;

    public RBDList(VectorAccessible accessible, BufferAllocator allocator) {
      super();
      this.accessible = accessible;
      this.allocator = allocator;
    }

    public void add() {
      RecordBatchData d = new RecordBatchData(accessible, allocator);
      batches.add(d);
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(batches);
    }

    public RecordBatches toRecordBatches() {
      return new RecordBatches(batches.stream()
        .map(t -> RecordBatchHolder.newRecordBatchHolder(t, 0, t.getRecordCount()))
        .collect(Collectors.toList())
      );
    }
  }
}
