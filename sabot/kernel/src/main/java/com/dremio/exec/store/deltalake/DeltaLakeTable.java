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

package com.dremio.exec.store.deltalake;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.common.HostAffinityComputer;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.parquet.ParquetGroupScanUtils;
import com.dremio.exec.store.schedule.EndpointByteMap;
import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf.DeltaLakeDatasetXAttr;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

/**
 * This class is responsible for orchestrating preparation of an overall snapshot by parsing oll Delta commit log files.
 */
@NotThreadSafe
public class DeltaLakeTable {
  private static final Logger logger = LoggerFactory.getLogger(DeltaLakeTable.class);

  private DeltaLogSnapshot deltaLogSnapshot = null;
  private final FileSystem fs;
  private final Path deltaLogDir;

  private long commitReadStartVersion = Long.MAX_VALUE;
  private long commitReadEndVersion = Long.MIN_VALUE;
  private long closestCheckpointVersion = -1L;
  private long closestLocalSnapshot = -1L;
  private final DeltaMetadataFetchJobManager manager;
  private final DeltaSnapshotListProcessor postProcessing = new DeltaSnapshotListProcessor();
  private final SabotContext context;
  private List<DeltaLogSnapshot> snapshots;

  public DeltaLakeTable(SabotContext context, FileSystem fs, FileSelection fileSelection,
                        TimeTravelOption.TimeTravelRequest travelRequest) {
    this(context, fs, fileSelection.getSelectionRoot(), travelRequest);
  }

  public DeltaLakeTable(SabotContext context, FileSystem fs, String selectionRoot,
                        TimeTravelOption.TimeTravelRequest travelRequest) {
      this.fs = fs;
      this.context = context;
      this.deltaLogDir = Path.of(selectionRoot).resolve(DeltaConstants.DELTA_LOG_DIR);
      this.manager = new DeltaMetadataFetchJobManager(context, fs, selectionRoot, travelRequest);
  }

    public DeltaLogSnapshot getConsolidatedSnapshot() throws IOException {
      List<DeltaLogSnapshot> snapshotList = getListOfSnapshot();

      deltaLogSnapshot = postProcessing.consolidateSnapshots(snapshotList);
      logger.debug("Final consolidated snapshot for delta dataset at {} is {}", deltaLogDir, deltaLogSnapshot);
      return deltaLogSnapshot;
    }

    public List<DatasetSplit> getAllSplits() {
      final List<DatasetSplit> allSplits = getListOfSnapshot().stream().sorted().flatMap(s -> s.getSplits().stream()).collect(Collectors.toList());
      if (allSplits.isEmpty()) {
          return allSplits;
      }
        // Reset first split with affinity information, to improve chances of cache hits in single threaded scenario
      final DatasetSplit firstSplit = refillWithAffinity(allSplits.get(0));
      allSplits.set(0, firstSplit);

      return allSplits;
    }

    private DatasetSplit refillWithAffinity(DatasetSplit inputSplit) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            inputSplit.getExtraInfo().writeTo(baos);
            final EasyProtobuf.EasyDatasetSplitXAttr xAttr = EasyProtobuf.EasyDatasetSplitXAttr.parseFrom(baos.toByteArray());
            final Path path = Path.of(xAttr.getPath());
            Iterable<FileBlockLocation> fileBlockLocations = fs.getFileBlockLocations(path, xAttr.getStart(), xAttr.getLength());
            final Map<HostAndPort, Float> affinities = HostAffinityComputer.computeAffinitiesForSplit(
              xAttr.getStart(), xAttr.getLength(), fileBlockLocations, fs.preserveBlockLocationsOrder());
            final List<DatasetSplitAffinity> datasetAffinities = new ArrayList<>();
            final Set<HostAndPort> hostEndpointMap = Sets.newHashSet();
            final Set<HostAndPort> hostPortEndpointMap = Sets.newHashSet();
            for (CoordinationProtos.NodeEndpoint endpoint : context.getExecutors()) {
                hostEndpointMap.add(HostAndPort.fromHost(endpoint.getAddress()));
                hostPortEndpointMap.add(HostAndPort.fromParts(endpoint.getAddress(), endpoint.getFabricPort()));
            }
            final EndpointByteMap endpointByteMap = ParquetGroupScanUtils.buildEndpointByteMap(hostEndpointMap, hostPortEndpointMap, affinities, xAttr.getLength());
            for (ObjectLongCursor<HostAndPort> item : endpointByteMap) {
                datasetAffinities.add(DatasetSplitAffinity.of(item.key.toString(), item.value));
            }
            logger.info("Affinities set up for {} are {}", xAttr.getPath(), datasetAffinities.size());
            return DatasetSplit.of(datasetAffinities, inputSplit.getSizeInBytes(), inputSplit.getRecordCount(), inputSplit.getExtraInfo());
        } catch (IOException ioe) {
            logger.error("Error while setting affinity");
            return inputSplit;
        }
    }

    // Use the last read version as the read signature
    public BytesOutput readSignature() throws IOException {
      if (!fs.isDirectory(deltaLogDir)) {
            throw new IllegalStateException("missing _delta_log directory for the DeltaLake table");
        }

        return DeltaLakeProtobuf
                .DeltaLakeReadSignature
                .newBuilder()
                .setCommitReadEndVersion(this.commitReadEndVersion)
                .build()::writeTo;
    }

    public DeltaLakeDatasetXAttr buildDatasetXattr() {
        final DeltaLakeDatasetXAttr.Builder deltaXAttrBuilder = DeltaLakeDatasetXAttr.newBuilder();
        deltaXAttrBuilder.setClosestCheckpointVersion(this.closestCheckpointVersion);
        deltaXAttrBuilder.setClosestLocalSnapshot(this.closestLocalSnapshot);
        deltaXAttrBuilder.setCommitReadStartVersion(this.commitReadStartVersion);
        deltaXAttrBuilder.setCommitReadEndVersion(this.commitReadEndVersion);
        deltaXAttrBuilder.setNumCommitJsonDataFileCount(setNumCommitJsonDataFileCount());

        if (deltaLogSnapshot != null) {
            deltaXAttrBuilder.setNumFiles(this.deltaLogSnapshot.getNetFilesAdded());
            deltaXAttrBuilder.setSizeBytes(this.deltaLogSnapshot.getNetBytesAdded());
        }
        return deltaXAttrBuilder.build();
    }

    private long setNumCommitJsonDataFileCount() {
      return getListOfSnapshot().stream().filter(s -> !s.containsCheckpoint()).mapToLong(DeltaLogSnapshot::getDataFileEntryCount).sum();
    }

    public boolean checkMetadataStale(DeltaLakeProtobuf.DeltaLakeReadSignature oldSignature) throws IOException {
      long oldVersion = oldSignature.getCommitReadEndVersion();

      if (oldVersion >= 0) {
        DeltaVersionResolver resolver = new DeltaVersionResolver(context, fs, deltaLogDir);
        boolean stale = resolver.getLastCheckpoint().getVersion() > oldVersion;
        Path newVersionFile = DeltaFilePathResolver.resolve(deltaLogDir, oldVersion + 1, 1, FileType.JSON).get(0);
        return stale || fs.exists(newVersionFile);
      }

      return true;
    }

    private List<DeltaLogSnapshot> getListOfSnapshot() {
      if(snapshots != null) {
        return snapshots;
      }

      snapshots = manager.getListOfSnapshots();
      snapshots = postProcessing.findValidSnapshots(snapshots);
      if(snapshots.size() > 0) {
        commitReadEndVersion = snapshots.get(0).getVersionId();
      }

      return snapshots;
    }
}
