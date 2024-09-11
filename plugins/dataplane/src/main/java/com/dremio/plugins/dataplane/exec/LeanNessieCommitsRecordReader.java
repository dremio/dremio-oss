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
package com.dremio.plugins.dataplane.exec;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.NessieCommitsSubScan;
import com.dremio.exec.store.iceberg.SnapshotEntry;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.OutputMutator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.VarCharVector;
import org.projectnessie.gc.contents.ContentReference;

/** Lean version of NessieCommitsRecordReader, which just identifies the metadata json paths. */
public class LeanNessieCommitsRecordReader extends AbstractNessieCommitRecordsReader {

  private VarCharVector metadataFilePathOutVector;

  public LeanNessieCommitsRecordReader(
      FragmentExecutionContext fragmentExecutionContext,
      OperatorContext context,
      NessieCommitsSubScan config) {
    super(fragmentExecutionContext, context, config);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    metadataFilePathOutVector = (VarCharVector) output.getVector(SystemSchemas.METADATA_FILE_PATH);
    super.setup(output);
  }

  @Override
  protected CompletableFuture<Optional<SnapshotEntry>> getEntries(
      AtomicInteger idx, ContentReference contentReference) {
    return CompletableFuture.completedFuture(
        Optional.of(
            new SnapshotEntry(
                contentReference.metadataLocation(), contentReference.snapshotId(), null, null)));
  }

  @Override
  protected void populateOutputVectors(AtomicInteger idx, SnapshotEntry snapshot) {
    byte[] metadataJsonPath = toSchemeAwarePath(snapshot.getMetadataJsonPath());
    metadataFilePathOutVector.setSafe(idx.getAndIncrement(), metadataJsonPath);
  }

  @Override
  protected void setValueCount(int valueCount) {
    metadataFilePathOutVector.setValueCount(valueCount);
  }
}
