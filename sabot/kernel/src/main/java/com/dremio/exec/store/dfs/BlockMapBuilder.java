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

import static com.dremio.telemetry.api.metrics.MeterProviders.newTimerResourceSampleSupplier;
import static com.dremio.telemetry.api.metrics.TimerUtils.timedExceptionThrowingOperation;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.store.dfs.easy.FileWork;
import com.dremio.exec.store.schedule.EndpointByteMap;
import com.dremio.exec.store.schedule.EndpointByteMapImpl;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.net.HostAndPort;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class BlockMapBuilder {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BlockMapBuilder.class);
  private static final Supplier<Timer.ResourceSample> BLOCK_MAP_BUILD_TIMER =
      newTimerResourceSampleSupplier(
          "block_map_builder",
          "Time taken to build a mapping of block locations to file byte range");

  private final Map<Path, ImmutableRangeMap<Long, FileBlockLocation>> blockMapMap =
      Maps.newConcurrentMap();
  private final CompressionCodecFactory codecFactory;
  private final FileSystem fs;
  private final ImmutableMap<String, NodeEndpoint> endPointMap;

  public BlockMapBuilder(
      CompressionCodecFactory codecFactory, FileSystem fs, Collection<NodeEndpoint> endpoints) {
    this.codecFactory = codecFactory;
    this.fs = fs;
    this.endPointMap = buildEndpointMap(endpoints);
  }

  private boolean compressed(FileAttributes fileAttributes) {
    return codecFactory.getCodec(fileAttributes.getPath()) != null;
  }

  public List<CompleteFileWork> generateFileWork(List<FileAttributes> files, boolean blockify)
      throws IOException {

    List<TimedRunnable<List<CompleteFileWork>>> readers = Lists.newArrayList();
    for (FileAttributes status : files) {
      readers.add(new BlockMapReader(status, blockify));
    }
    List<List<CompleteFileWork>> work = TimedRunnable.run("Get block maps", logger, readers, 16);
    List<CompleteFileWork> singleList = Lists.newArrayList();
    for (List<CompleteFileWork> innerWorkList : work) {
      singleList.addAll(innerWorkList);
    }

    return singleList;
  }

  private final class BlockMapReader extends TimedRunnable<List<CompleteFileWork>> {
    private final FileAttributes attributes;

    // This variable blockify indicates if a single file can be read by multiple threads
    // For examples, for CSV, it is set as true
    // because each row in a CSV file can be considered as an independent record;
    // for json, it is set as false
    // because each row in a json file cannot be determined as a record or not simply by that row
    // alone
    private final boolean blockify;

    private BlockMapReader(FileAttributes attributes, boolean blockify) {
      super();
      this.attributes = attributes;
      this.blockify = blockify;
    }

    @Override
    protected List<CompleteFileWork> runInner() throws Exception {
      final List<CompleteFileWork> work = Lists.newArrayList();
      boolean error = false;
      if (blockify && !compressed(attributes)) {
        try {
          ImmutableRangeMap<Long, FileBlockLocation> rangeMap = getBlockMap(attributes);
          for (Entry<Range<Long>, FileBlockLocation> l : rangeMap.asMapOfRanges().entrySet()) {
            work.add(
                new CompleteFileWork(
                    getEndpointByteMap(
                        new FileAttributesWork(
                            attributes, l.getValue().getOffset(), l.getValue().getSize())),
                    l.getValue().getOffset(),
                    l.getValue().getSize(),
                    attributes));
          }
        } catch (IOException e) {
          logger.warn("failure while generating file work.", e);
          error = true;
        }
      }

      if (!blockify || error || compressed(attributes)) {
        work.add(
            new CompleteFileWork(
                getEndpointByteMap(new FileAttributesWork(attributes)),
                0,
                attributes.size(),
                attributes));
      }

      // This if-condition is specific for empty CSV file
      // For CSV files, the global variable blockify is set as true
      // And if this CSV file is empty, rangeMap would be empty also
      // Therefore, at the point before this if-condition, work would not be populated
      if (work.isEmpty()) {
        work.add(
            new CompleteFileWork(
                getEndpointByteMap(new FileAttributesWork(attributes)), 0, 0, attributes));
      }

      return work;
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      return new IOException(
          "Failure while trying to get block map for " + attributes.getPath(), e);
    }
  }

  private static final class FileAttributesWork implements FileWork {
    private final FileAttributes status;
    private final long start;
    private final long length;

    public FileAttributesWork(FileAttributes status) {
      this(status, 0, status.size());
    }

    public FileAttributesWork(FileAttributes status, long start, long length) {
      Preconditions.checkArgument(
          !status.isDirectory(), "FileStatus work only works with files, not directories.");
      this.status = status;
      this.start = start;
      this.length = length;
    }

    @Override
    public FileAttributes getFileAttributes() {
      return status;
    }

    @Override
    public long getStart() {
      return start;
    }

    @Override
    public long getLength() {
      return length;
    }
  }

  /** Builds a mapping of block locations to file byte range */
  private ImmutableRangeMap<Long, FileBlockLocation> buildBlockMap(FileAttributes attributes)
      throws IOException {
    return timedExceptionThrowingOperation(
        BLOCK_MAP_BUILD_TIMER.get(),
        () -> {
          Iterable<FileBlockLocation> blocks;
          ImmutableRangeMap<Long, FileBlockLocation> blockMap;
          blocks = fs.getFileBlockLocations(attributes, 0, attributes.size());
          ImmutableRangeMap.Builder<Long, FileBlockLocation> blockMapBuilder =
              new ImmutableRangeMap.Builder<>();
          for (FileBlockLocation block : blocks) {
            long start = block.getOffset();
            long end = start + block.getSize();
            Range<Long> range = Range.closedOpen(start, end);
            blockMapBuilder = blockMapBuilder.put(range, block);
          }
          blockMap = blockMapBuilder.build();
          blockMapMap.put(attributes.getPath(), blockMap);
          return blockMap;
        });
  }

  private ImmutableRangeMap<Long, FileBlockLocation> getBlockMap(FileAttributes attributes)
      throws IOException {
    ImmutableRangeMap<Long, FileBlockLocation> blockMap = blockMapMap.get(attributes.getPath());
    if (blockMap == null) {
      blockMap = buildBlockMap(attributes);
    }
    return blockMap;
  }

  /**
   * For a given FileWork, calculate how many bytes are available on each on node endpoint
   *
   * @param work the FileWork to calculate endpoint bytes for
   * @throws IOException
   */
  public EndpointByteMap getEndpointByteMap(FileWork work) throws IOException {
    Stopwatch watch = Stopwatch.createStarted();

    ImmutableRangeMap<Long, FileBlockLocation> blockMap = getBlockMap(work.getFileAttributes());
    EndpointByteMapImpl endpointByteMap = new EndpointByteMapImpl();
    long start = work.getStart();
    long end = start + work.getLength();
    Range<Long> rowGroupRange = Range.closedOpen(start, end);

    // Find submap of ranges that intersect with the rowGroup
    ImmutableRangeMap<Long, FileBlockLocation> subRangeMap = blockMap.subRangeMap(rowGroupRange);

    // Iterate through each block in this submap and get the host for the block location
    for (Map.Entry<Range<Long>, FileBlockLocation> block : subRangeMap.asMapOfRanges().entrySet()) {
      List<String> hosts = block.getValue().getHosts();
      Range<Long> blockRange = block.getKey();
      Range<Long> intersection = rowGroupRange.intersection(blockRange);
      long bytes = intersection.upperEndpoint() - intersection.lowerEndpoint();

      // For each host in the current block location, add the intersecting bytes to the
      // corresponding endpoint
      for (String host : hosts) {
        NodeEndpoint endpoint = getNodeEndpoint(host);
        if (endpoint != null) {
          endpointByteMap.add(HostAndPort.fromHost(endpoint.getAddress()), bytes);
        } else {
          logger.debug(
              "Failure finding SabotNode running on host {}.  Skipping affinity to that host.",
              host);
        }
      }
    }

    logger.debug(
        "FileWork group ({},{}) max bytes {}",
        work.getFileAttributes().getPath(),
        work.getStart(),
        endpointByteMap.getMaxBytes());

    logger.debug("Took {} ms to set endpoint bytes", watch.stop().elapsed(TimeUnit.MILLISECONDS));
    return endpointByteMap;
  }

  private NodeEndpoint getNodeEndpoint(String hostName) {
    return endPointMap.get(hostName);
  }

  /** Builds a mapping of SabotNode endpoints to hostnames */
  private static ImmutableMap<String, NodeEndpoint> buildEndpointMap(
      Collection<NodeEndpoint> endpoints) {
    Stopwatch watch = Stopwatch.createStarted();
    Map<String, NodeEndpoint> endpointMap = Maps.newHashMap();
    for (NodeEndpoint d : endpoints) {
      String hostName = d.getAddress();
      endpointMap.put(hostName, d);
    }
    watch.stop();
    logger.debug("Took {} ms to build endpoint map", watch.elapsed(TimeUnit.MILLISECONDS));
    return ImmutableMap.copyOf(endpointMap);
  }
}
