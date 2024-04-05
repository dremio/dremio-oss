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

import static com.dremio.exec.store.IcebergExpiryMetric.COMMIT_SCAN_TIME;
import static org.projectnessie.gc.contents.LiveContentSet.Status.IDENTIFY_SUCCESS;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.IcebergExpiryMetric;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.iceberg.NessieCommitsSubScan;
import com.dremio.exec.store.iceberg.SnapshotEntry;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.iceberg.IcebergContentToContentReference;
import org.projectnessie.gc.iceberg.IcebergContentTypeFilter;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.identify.IdentifyLiveContents;
import org.projectnessie.gc.repository.RepositoryConnector;

public abstract class AbstractNessieCommitRecordsReader implements RecordReader {

  private final NessieCommitsSubScan config;
  private final FragmentExecutionContext fragmentExecutionContext;
  private final OperatorContext context;
  private SupportsIcebergMutablePlugin plugin;

  private InMemoryPersistenceSpi persistenceSpi;
  private UUID liveContentId;
  private LiveContentSet liveContentSet;
  private Iterator<String> liveContentSetIterator;
  private Iterator<ContentReference> contentRefsIterator = Collections.emptyIterator();

  public AbstractNessieCommitRecordsReader(
      FragmentExecutionContext fragmentExecutionContext,
      OperatorContext context,
      NessieCommitsSubScan config) {
    this.fragmentExecutionContext = fragmentExecutionContext;
    this.context = context;
    this.config = config;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    plugin = fragmentExecutionContext.getStoragePlugin(config.getPluginId());
    Preconditions.checkState(
        plugin instanceof DataplanePlugin,
        "Unsupported plugin type: " + plugin.getClass().getName());
    NessieApiV2 nessieApi = ((DataplanePlugin) plugin).getNessieApi();

    SnapshotsScanOptions snapshotsScanOptions = config.getSnapshotsScanOptions();
    persistenceSpi = new InMemoryPersistenceSpi();
    RepositoryConnector repositoryConnector =
        new QueryContextAwareNessieRepositoryConnector(
            context.getFragmentHandle().getQueryId(), nessieApi);

    Stopwatch liveContentScanWatch = Stopwatch.createStarted();

    liveContentId =
        IdentifyLiveContents.builder()
            .repositoryConnector(repositoryConnector)
            .liveContentSetsRepository(
                LiveContentSetsRepository.builder().persistenceSpi(persistenceSpi).build())
            .cutOffPolicySupplier(
                reference ->
                    CutoffPolicy.atTimestamp(
                        Instant.ofEpochMilli(snapshotsScanOptions.getOlderThanInMillis())))
            .contentTypeFilter(IcebergContentTypeFilter.INSTANCE)
            .contentToContentReference(IcebergContentToContentReference.INSTANCE)
            .build()
            .identifyLiveContents();

    try {
      liveContentSet = persistenceSpi.getLiveContentSet(liveContentId);
      Preconditions.checkState(
          liveContentSet.status().equals(IDENTIFY_SUCCESS),
          "Error while identifying live contents.");

      context
          .getStats()
          .addLongStat(
              IcebergExpiryMetric.NUM_TABLES, liveContentSet.fetchDistinctContentIdCount());
      try (Stream<String> stream = liveContentSet.fetchContentIds()) {
        liveContentSetIterator = stream.iterator();
      }
    } catch (LiveContentSetNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
      context
          .getStats()
          .addLongStat(COMMIT_SCAN_TIME, liveContentScanWatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public int next() {
    int idx = 0;

    while ((liveContentSetIterator.hasNext() || contentRefsIterator.hasNext())
        && idx < context.getTargetBatchSize()) {
      if (!contentRefsIterator.hasNext()) {
        String contentSet = liveContentSetIterator.next();
        contentRefsIterator =
            persistenceSpi.fetchContentReferences(liveContentId, contentSet).iterator();
      }
      idx = publishRecords(idx);
    }

    setValueCount(idx);
    return idx;
  }

  private int publishRecords(int startIdx) {
    final AtomicInteger idx = new AtomicInteger(startIdx);
    int runningIdx = startIdx;
    List<CompletableFuture<Optional<SnapshotEntry>>> snapshotEntries = new ArrayList<>();
    while (contentRefsIterator.hasNext() && runningIdx++ < context.getTargetBatchSize()) {
      snapshotEntries.add(getEntries(idx, contentRefsIterator.next()));
    }
    snapshotEntries.forEach(e -> e.join().ifPresent(se -> populateOutputVectors(idx, se)));
    return idx.intValue();
  }

  protected abstract CompletableFuture<Optional<SnapshotEntry>> getEntries(
      AtomicInteger idx, ContentReference next);

  protected abstract void populateOutputVectors(AtomicInteger idx, SnapshotEntry entry);

  protected abstract void setValueCount(int valueCount);

  @Override
  public void close() {
    liveContentSet.delete();
  }

  protected NessieCommitsSubScan getConfig() {
    return config;
  }

  protected OperatorContext getContext() {
    return context;
  }

  protected SupportsIcebergMutablePlugin getPlugin() {
    return plugin;
  }
}
