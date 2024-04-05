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
package com.dremio.exec.store.dfs;

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.cache.BlockLocationsCacheManager;
import com.dremio.exec.store.common.EndpointsAffinity;
import com.dremio.exec.store.common.HostAffinityComputer;
import com.dremio.exec.store.common.InitializationException;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.schedule.AssignmentCreator2;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.exec.util.rhash.RendezvousHash;
import com.dremio.exec.util.rhash.RendezvousPageHasher;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.Hashing;
import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * SplitAssignmentTableFunction is responsible for assigning the splits to the downstream fragment
 * endpoints The incoming batch is expected to contain a varbinary column named "splitsIdentity"
 * that contains block location information of the splits. The outgoing batch contains a single
 * additional int column apart from the incoming batch called "E_X_P_R_H_A_S_H_F_I_E_L_D" which
 * contains the target fragment index for that particular split.
 */
public class SplitAssignmentTableFunction extends AbstractTableFunction {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SplitAssignmentTableFunction.class);
  private static final long REPEAT_LOG_THRESHOLD = Duration.ofMinutes(30).toMillis();
  private static long lastLoggedSetupExceptionTime;

  private IntVector hashVector;
  private StructVector inputSplitIdentities;
  private int currentRow;
  private final List<TransferPair> transfers = new ArrayList<>();
  private boolean assigned = false;
  private final List<NodeEndpoint> nodeEndpoints;
  private final Map<HostAndPort, NodeEndpoint> hostAndPortToEndpointMap;
  private final Multimap<HostAndPort, NodeEndpoint>
      hostToEndpointMap; // only keep the host information in this map
  private List<SplitWork> splitWorkList;
  private final double balanceFactor;
  private final RendezvousHash<RendezvousPageHasher.PathOffset, ComparableEndpoint> hasher;

  private final SupportsInternalIcebergTable plugin;
  private final StoragePluginId pluginId;
  private final OpProps props;
  private Pointer<BlockLocationsCacheManager> cacheManagerPointer;
  private FileSystem fs;

  public SplitAssignmentTableFunction(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig)
      throws ExecutionSetupException {
    super(context, functionConfig);

    if (functionConfig.getFunctionContext().isIcebergMetadata()
        && functionConfig.getFunctionContext().getInternalTablePluginId() != null) {
      this.pluginId = functionConfig.getFunctionContext().getInternalTablePluginId();
      this.plugin = fec.getStoragePlugin(pluginId);
    } else {
      this.pluginId = functionConfig.getFunctionContext().getPluginId();
      this.plugin = IcebergUtils.getSupportsInternalIcebergTablePlugin(fec, pluginId);
    }
    this.props = props;

    nodeEndpoints =
        context.getMinorFragmentEndpoints().stream()
            .sorted(Comparator.comparing(MinorFragmentEndpoint::getMinorFragmentId))
            .map(MinorFragmentEndpoint::getEndpoint)
            .collect(Collectors.toList());

    hostAndPortToEndpointMap =
        nodeEndpoints.stream()
            .collect(
                Collectors.toMap(
                    e -> HostAndPort.fromParts(e.getAddress(), e.getFabricPort()),
                    Function.identity(),
                    (a, b) -> a));
    hostToEndpointMap = ArrayListMultimap.create();
    hostAndPortToEndpointMap
        .entrySet()
        .forEach(
            e -> hostToEndpointMap.put(HostAndPort.fromHost(e.getKey().getHost()), e.getValue()));

    Set<ComparableEndpoint> comparableEndpoints =
        nodeEndpoints.stream()
            .map(
                n ->
                    new ComparableEndpoint(
                        HostAndPort.fromParts(n.getAddress(), n.getFabricPort())))
            .collect(Collectors.toSet());
    hasher =
        new RendezvousHash<>(
            Hashing.murmur3_128(),
            (k, f) -> f.putString(k.getPath(), StandardCharsets.UTF_8).putLong(k.getOffset()),
            (n, f) ->
                f.putString(n.hostPort.getHost(), StandardCharsets.UTF_8)
                    .putInt(n.hostPort.getPort()),
            comparableEndpoints);
    balanceFactor = context.getOptions().getOption(ExecConstants.ASSIGNMENT_CREATOR_BALANCE_FACTOR);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    this.incoming = accessible;
    this.outgoing = context.createOutputVectorContainer();
    functionConfig.getOutputSchema().getFields().forEach(f -> outgoing.addOrGet(f));
    outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    hashVector = (IntVector) getVectorFromSchemaPath(outgoing, HashPrelUtil.HASH_EXPR_NAME);
    inputSplitIdentities =
        (StructVector) getVectorFromSchemaPath(incoming, RecordReader.SPLIT_IDENTITY);

    for (Field field : incoming.getSchema()) {
      ValueVector vvIn = getVectorFromSchemaPath(incoming, field.getName());
      ValueVector vvOut = getVectorFromSchemaPath(outgoing, field.getName());
      TransferPair tp = vvIn.makeTransferPair(vvOut);
      transfers.add(tp);
    }
    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    currentRow = row;
    if (row != 0) {
      return;
    }
    assigned = false;

    int batchSize = incoming.getRecordCount();
    if (batchSize == 0) {
      return;
    }

    List<SplitIdentity> splitIdentities = new ArrayList<>();
    List<PartitionProtobuf.BlockLocationsList> blockLocationsLists = new ArrayList<>();

    String lastPath = null;
    PartitionProtobuf.BlockLocationsList lastBlockLocations = null;
    for (int record = 0; record < batchSize; ++record) {
      if (inputSplitIdentities.isNull(record)) {
        continue;
      }
      Map<String, Object> fields = (Map<String, Object>) inputSplitIdentities.getObject(record);
      String path = fields.get(SplitIdentity.PATH).toString();
      long offset = (Long) fields.get(SplitIdentity.OFFSET);
      long length = (Long) fields.get(SplitIdentity.LENGTH);
      long fileLength = (Long) fields.get(SplitIdentity.FILE_LENGTH);

      SplitIdentity splitIdentity = new SplitIdentity(path, offset, length, fileLength);
      splitIdentities.add(splitIdentity);

      // Since during producing the splits corresponding to a single file are added together
      // we can query the cache for block locations only the first time we encounter a new file
      if (!path.equals(lastPath)) {
        lastBlockLocations = getFileBlockLocations(path, fileLength);
        lastPath = path;
      }
      blockLocationsLists.add(lastBlockLocations);
    }

    splitWorkList =
        IntStream.range(0, splitIdentities.size())
            .mapToObj(i -> new SplitWork(splitIdentities.get(i), blockLocationsLists.get(i), i))
            .collect(Collectors.toList());
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if (currentRow != 0 || assigned) {
      return 0;
    }

    int recordCount = incoming.getRecordCount();
    Preconditions.checkArgument(recordCount <= maxRecords);

    AssignmentCreator2.getMappings(nodeEndpoints, splitWorkList, balanceFactor)
        .asMap()
        .entrySet()
        .stream()
        .flatMap(e -> e.getValue().stream().map(k -> Pair.of(k.rowIndex, e.getKey())))
        .sorted(Comparator.comparing(Pair::getLeft))
        .forEach(p -> hashVector.setSafe(p.getLeft(), p.getRight()));
    transfers.forEach(TransferPair::transfer);
    outgoing.setAllCount(recordCount);
    assigned = true;
    return recordCount;
  }

  @Override
  public void closeRow() throws Exception {}

  @Override
  public void close() throws Exception {
    AutoCloseables.close(super::close);
    if (cacheManagerPointer != null) {
      AutoCloseables.close(cacheManagerPointer.value);
    }
    this.context.getStats().setReadIOStats();
  }

  @VisibleForTesting
  PartitionProtobuf.BlockLocationsList getFileBlockLocations(String filePath, long fileSize) {
    if (cacheManagerPointer == null) {
      try {
        DremioConfig dremioConfig = context.getDremioConfig();
        String id = pluginId.getConfig().getId().getId();
        /*
         * There are 3 scenarios to consider:
         * 1. cache manager is created correctly
         *    - cacheManager will be non-null
         *    - cacheManagerPointer will be non-null. This object will not be created again.
         *    - files can be cached
         * 2. cache manager is not created (e.g. because of a permission error on the filesystem)
         *    - BlockLocationsCacheManager.newInstance will throw exception, which will be logged judiciously
         *    - cacheManager and cacheManagerPointer will be null. This object will be created again by future calls.
         *    - files will be cached only when one of the future calls successfully creates cache manager
         * 3. cache manager is not created (e.g. because the filesystem is not HDFS)
         *    - cacheManager will be null
         *    - cacheManagerPointer will be non-null. This object will not be created again.
         *    - files will not be cached
         */
        BlockLocationsCacheManager cacheManager =
            BlockLocationsCacheManager.newInstance(getFs(filePath), id, dremioConfig, context);
        cacheManagerPointer = new Pointer<>(cacheManager);
      } catch (InitializationException e) {
        logJudiciously(e);
      }
    }

    if (cacheManagerPointer != null && cacheManagerPointer.value != null) {
      return cacheManagerPointer.value.createIfAbsent(filePath, fileSize);
    }
    return null;
  }

  private static void logJudiciously(Exception e) {
    // write judiciously to prevent flooding the logs with multiple such messages
    long currTimeMillis = System.currentTimeMillis();
    long diff = currTimeMillis - lastLoggedSetupExceptionTime;
    if (lastLoggedSetupExceptionTime == 0 || diff > REPEAT_LOG_THRESHOLD) {
      logger.error("Unable to create block locations cache manager", e);
      lastLoggedSetupExceptionTime = currTimeMillis;
    }
  }

  private FileSystem getFs(String filePath) {
    if (fs == null) {
      try {
        fs = plugin.createFS(filePath, props.getUserName(), context);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return fs;
  }

  private class SplitWork implements CompleteWork {

    private final SplitIdentity splitIdentity;
    private final int rowIndex;
    private final List<EndpointAffinity> affinity;

    SplitWork(
        SplitIdentity splitIdentity,
        PartitionProtobuf.BlockLocationsList blockLocationsList,
        int rowIndex) {
      this.splitIdentity = splitIdentity;
      this.rowIndex = rowIndex;
      this.affinity = deriveAffinity(splitIdentity, blockLocationsList);
    }

    private List<EndpointAffinity> deriveAffinity(
        SplitIdentity splitIdentity, PartitionProtobuf.BlockLocationsList blockLocations) {
      if (blockLocations != null && blockLocations.getBlockLocationsCount() != 0) {
        List<EndpointsAffinity> affinities =
            HostAffinityComputer.computeSortedAffinitiesForSplit(
                splitIdentity.getOffset(),
                splitIdentity.getLength(),
                blockLocations.getBlockLocationsList());
        boolean isInstanceAffinity =
            !affinities.isEmpty() && affinities.get(0).isInstanceAffinity();

        Set<HostAndPort> hostAndPortsSet =
            isInstanceAffinity ? hostAndPortToEndpointMap.keySet() : hostToEndpointMap.keySet();
        return affinities.stream()
            .filter(a -> hostAndPortsSet.contains(a.getHostAndPort()))
            .flatMap(
                a ->
                    isInstanceAffinity
                        ? Stream.of(
                            new EndpointAffinity(
                                a.getHostAndPort(),
                                hostAndPortToEndpointMap.get(a.getHostAndPort()),
                                a.getAffinity(),
                                false,
                                Integer.MAX_VALUE))
                        : hostToEndpointMap.get(a.getHostAndPort()).stream()
                            .map(
                                endpoint ->
                                    new EndpointAffinity(
                                        a.getHostAndPort(),
                                        endpoint,
                                        a.getAffinity(),
                                        false,
                                        Integer.MAX_VALUE)))
            .collect(Collectors.toList());
      } else {
        ComparableEndpoint comparableEndpoint =
            hasher.get(
                new RendezvousPageHasher.PathOffset(
                    splitIdentity.getPath(), splitIdentity.getOffset()));
        NodeEndpoint nodeEndpoint = hostAndPortToEndpointMap.get(comparableEndpoint.hostPort);
        return Collections.singletonList(
            new EndpointAffinity(
                comparableEndpoint.hostPort, nodeEndpoint, 1.0, false, Integer.MAX_VALUE));
      }
    }

    @Override
    public long getTotalBytes() {
      return splitIdentity.getLength();
    }

    @Override
    public List<EndpointAffinity> getAffinity() {
      return this.affinity;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return Long.compare(getTotalBytes(), o.getTotalBytes());
    }
  }

  private class ComparableEndpoint implements Comparable<ComparableEndpoint> {
    private final HostAndPort hostPort;

    ComparableEndpoint(HostAndPort hostAndPort) {
      this.hostPort = hostAndPort;
    }

    @Override
    public int compareTo(SplitAssignmentTableFunction.ComparableEndpoint o) {
      int result = hostPort.getHost().compareTo(o.hostPort.getHost());
      if (result == 0 && hostPort.hasPort() && o.hostPort.hasPort()) {
        result = Integer.compare(hostPort.getPort(), o.hostPort.getPort());
      }
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ComparableEndpoint that = (ComparableEndpoint) o;
      return hostPort.equals(that.hostPort);
    }

    @Override
    public int hashCode() {
      return hostPort.hashCode();
    }
  }
}
