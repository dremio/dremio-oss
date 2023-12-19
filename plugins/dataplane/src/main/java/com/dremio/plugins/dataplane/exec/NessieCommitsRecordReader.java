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

import static com.dremio.exec.store.IcebergExpiryMetric.NUM_ACCESS_DENIED;
import static com.dremio.exec.store.IcebergExpiryMetric.NUM_NOT_FOUND;
import static com.dremio.exec.store.IcebergExpiryMetric.NUM_PARTIAL_FAILURES;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.gc.contents.ContentReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.IcebergExpiryMetric;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.NessieCommitsSubScan;
import com.dremio.exec.store.iceberg.SnapshotEntry;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

/**
 * Scans all live Nessie contents, and outputs the metadata locations for each one of them.
 */
public class NessieCommitsRecordReader extends AbstractNessieCommitRecordsReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieCommitsRecordReader.class);

  private volatile VarCharVector metadataFilePathOutVector;
  private volatile BigIntVector snapshotIdOutVector;
  private volatile VarCharVector manifestListPathOutVector;
  private FileIO io = null;
  private final ExecutorService opExecService;

  public NessieCommitsRecordReader(FragmentExecutionContext fec,
                                   OperatorContext context,
                                   NessieCommitsSubScan config) {
    super(fec, context, config);
    opExecService = context.getExecutor();
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    metadataFilePathOutVector = (VarCharVector) output.getVector(SystemSchemas.METADATA_FILE_PATH);
    snapshotIdOutVector = (BigIntVector) output.getVector(SystemSchemas.SNAPSHOT_ID);
    manifestListPathOutVector = (VarCharVector) output.getVector(SystemSchemas.MANIFEST_LIST_PATH);

    super.setup(output);
  }

  @Override
  protected CompletableFuture<Optional<SnapshotEntry>> getEntries(AtomicInteger idx, ContentReference contentReference) {
    return CompletableFuture.supplyAsync(
      () -> tryLoadSnapshot(contentReference).map(s -> new SnapshotEntry(contentReference.metadataLocation(), s)),
      opExecService);
  }

  @Override
  protected void populateOutputVectors(AtomicInteger idx, SnapshotEntry snapshot) {
    final int idxVal = idx.getAndIncrement();
    metadataFilePathOutVector.setSafe(idxVal, snapshot.getMetadataJsonPath().getBytes(StandardCharsets.UTF_8));
    snapshotIdOutVector.setSafe(idxVal, snapshot.getSnapshotId());
    manifestListPathOutVector.setSafe(idxVal, snapshot.getManifestListPath().getBytes(StandardCharsets.UTF_8));
  }

  @Override
  protected void setValueCount(int valueCount) {
    metadataFilePathOutVector.setValueCount(valueCount);
    snapshotIdOutVector.setValueCount(valueCount);
    manifestListPathOutVector.setValueCount(valueCount);
  }

  private Optional<Snapshot> tryLoadSnapshot(ContentReference contentReference) {
    if (contentReference.snapshotId() == null || contentReference.snapshotId() == -1) {
      return Optional.empty();
    }
    String tableId = String.format("%s@%d AT %s", contentReference.contentKey(), contentReference.snapshotId(), contentReference.commitId());
    Stopwatch loadTime = Stopwatch.createStarted();
    try {
      return Optional.of(loadSnapshot(contentReference.metadataLocation(), contentReference.snapshotId()));
    } catch (NotFoundException nfe) {
      LOGGER.warn(String.format("Skipping table [%s] since table metadata is not found [metadata=%s]", tableId, contentReference.metadataLocation()), nfe);
      getContext().getStats().addLongStat(NUM_PARTIAL_FAILURES, 1L);
      getContext().getStats().addLongStat(NUM_NOT_FOUND, 1L);
      return Optional.empty();
    } catch (UserException e) {
      if (UserBitShared.DremioPBError.ErrorType.PERMISSION.equals(e.getErrorType())) {
        LOGGER.warn(String.format("Skipping table [%s] since access to table metadata is denied [metadata=%s]", tableId, contentReference.metadataLocation()), e);
        getContext().getStats().addLongStat(NUM_PARTIAL_FAILURES, 1L);
        getContext().getStats().addLongStat(NUM_ACCESS_DENIED, 1L);
        return Optional.empty();
      }

      throw e;
    } catch (IOException ioe) {
      throw UserException.ioExceptionError(ioe).message("Error while loading the snapshot %d from table %s on commit %s",
        contentReference.snapshotId(), contentReference.contentKey(), contentReference.commitId()).build();
    } finally {
      LOGGER.debug("{} load time {}ms", tableId, loadTime.elapsed(TimeUnit.MILLISECONDS));
      getContext().getStats().addLongStat(IcebergExpiryMetric.SNAPSHOT_LOAD_TIME, loadTime.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  @VisibleForTesting
  Snapshot loadSnapshot(String metadataJsonPath, long snapshotId) throws IOException {
    return TableMetadataParser.read(io(metadataJsonPath), metadataJsonPath).snapshot(snapshotId);
  }

  private FileIO io(String metadataLocation) throws IOException {
    if (io == null) {
      FileSystem fs = getPlugin().createFSWithAsyncOptions(metadataLocation, getConfig().getProps().getUserName(),
        getContext());
      io = getPlugin().createIcebergFileIO(fs, getContext(), null, getConfig().getPluginId().getName(), null);
    }
    return io;
  }
}
