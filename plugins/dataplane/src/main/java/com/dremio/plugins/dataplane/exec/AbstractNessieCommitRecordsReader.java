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
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.NessieCommitsSubScan;
import com.dremio.exec.store.iceberg.SnapshotEntry;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.plugins.dataplane.exec.gc.NessieLiveContentRetriever;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractNessieCommitRecordsReader implements RecordReader {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractNessieCommitRecordsReader.class);

  private final NessieCommitsSubScan config;
  private final FragmentExecutionContext fragmentExecutionContext;
  private final OperatorContext context;
  private SupportsIcebergMutablePlugin plugin;

  private LiveContentSet liveContentSet;
  private Iterator<String> liveContentIdsSetIterator;
  private Iterator<ContentReference> contentRefsIterator = Collections.emptyIterator();
  private Configuration conf;

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
    this.conf = plugin.getFsConfCopy();
    NessieApiV2 nessieApi = ((DataplanePlugin) plugin).getNessieApi();

    SnapshotsScanOptions snapshotsScanOptions = config.getSnapshotsScanOptions();
    RepositoryConnector repositoryConnector =
        new QueryContextAwareNessieRepositoryConnector(
            context.getFragmentHandle().getQueryId(), nessieApi);

    Stopwatch liveContentScanWatch = Stopwatch.createStarted();

    NessieLiveContentRetriever nessieLiveContentRetriever =
        NessieLiveContentRetriever.create(repositoryConnector);

    retrieveLiveContentSet(nessieLiveContentRetriever, snapshotsScanOptions, liveContentScanWatch);
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

    while ((liveContentIdsSetIterator.hasNext() || contentRefsIterator.hasNext())
        && idx < context.getTargetBatchSize()) {
      idx = publishNextRecords(idx);
    }

    if (!liveContentIdsSetIterator.hasNext() && !contentRefsIterator.hasNext()) {
      deleteLiveContentSet();
    }

    setValueCount(idx);
    return idx;
  }

  private int publishNextRecords(int startIdx) {
    if (!contentRefsIterator.hasNext()) {
      String contentSet = liveContentIdsSetIterator.next();
      // todo: we will need to be aware of closing the resources with our own DiskPersistenceSpi
      try (Stream<ContentReference> contentReference =
          liveContentSet.fetchContentReferences(contentSet)) {
        contentRefsIterator = contentReference.iterator();
      }
    }
    return publishRecords(startIdx);
  }

  private void deleteLiveContentSet() {
    try {
      liveContentSet.delete();
    } catch (IllegalStateException exception) {
      // skip if no live content found
      // todo: in our implementation of PersistenceSpi that saves
      // the live content to the disk, we can check there for the existence of the live set
      // and just throw not found exception
      LOGGER.debug("Ignoring deletion of live content set due to not found exception", exception);
    }
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

  @Override
  public void close() {}

  protected abstract CompletableFuture<Optional<SnapshotEntry>> getEntries(
      AtomicInteger idx, ContentReference next);

  protected abstract void populateOutputVectors(AtomicInteger idx, SnapshotEntry entry);

  protected abstract void setValueCount(int valueCount);

  protected NessieCommitsSubScan getConfig() {
    return config;
  }

  protected OperatorContext getContext() {
    return context;
  }

  protected SupportsIcebergMutablePlugin getPlugin() {
    return plugin;
  }

  protected byte[] toSchemeAwarePath(String path) {
    String schemeAwarePath =
        IcebergUtils.getIcebergPathAndValidateScheme(
            path, conf, config.getFsScheme(), config.getSchemeVariate());
    return schemeAwarePath.getBytes(StandardCharsets.UTF_8);
  }

  private void retrieveLiveContentSet(
      NessieLiveContentRetriever nessieLiveContentRetriever,
      SnapshotsScanOptions snapshotsScanOptions,
      Stopwatch liveContentScanWatch) {
    try {
      liveContentSet =
          nessieLiveContentRetriever.getLiveContentSet(
              CutoffPolicy.atTimestamp(
                  Instant.ofEpochMilli(snapshotsScanOptions.getOlderThanInMillis())));

      Preconditions.checkState(
          liveContentSet.status().equals(IDENTIFY_SUCCESS),
          "Error while identifying live contents.");

      context
          .getStats()
          .addLongStat(
              IcebergExpiryMetric.NUM_TABLES, liveContentSet.fetchDistinctContentIdCount());

      try (Stream<String> stream = liveContentSet.fetchContentIds()) {
        liveContentIdsSetIterator = stream.iterator();
      }
    } catch (LiveContentSetNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
      context
          .getStats()
          .addLongStat(COMMIT_SCAN_TIME, liveContentScanWatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }
}
