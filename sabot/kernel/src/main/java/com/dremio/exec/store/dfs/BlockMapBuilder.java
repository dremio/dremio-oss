/*
 * Copyright (C) 2017-2018 Dremio Corporation
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


import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.TimedRunnable;
import com.dremio.exec.store.dfs.easy.FileWork;
import com.dremio.exec.store.schedule.EndpointByteMap;
import com.dremio.exec.store.schedule.EndpointByteMapImpl;
import com.dremio.metrics.Metrics;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

public class BlockMapBuilder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BlockMapBuilder.class);
  static final MetricRegistry metrics = Metrics.getInstance();
  static final String BLOCK_MAP_BUILDER_TIMER = MetricRegistry.name(BlockMapBuilder.class, "blockMapBuilderTimer");

  private final Map<Path,ImmutableRangeMap<Long,BlockLocation>> blockMapMap = Maps.newConcurrentMap();
  private final FileSystem fs;
  private final ImmutableMap<String,NodeEndpoint> endPointMap;
  private final CompressionCodecFactory codecFactory;

  public BlockMapBuilder(FileSystem fs, Collection<NodeEndpoint> endpoints) {
    this.fs = fs;
    this.codecFactory = new CompressionCodecFactory(fs.getConf());
    this.endPointMap = buildEndpointMap(endpoints);
  }

  private boolean compressed(FileStatus fileStatus) {
    return codecFactory.getCodec(fileStatus.getPath()) != null;
  }

  public List<CompleteFileWork> generateFileWork(List<FileStatus> files, boolean blockify) throws IOException {

    List<TimedRunnable<List<CompleteFileWork>>> readers = Lists.newArrayList();
    for(FileStatus status : files){
      readers.add(new BlockMapReader(status, blockify));
    }
    List<List<CompleteFileWork>> work = TimedRunnable.run("Get block maps", logger, readers, 16);
    List<CompleteFileWork> singleList = Lists.newArrayList();
    for(List<CompleteFileWork> innerWorkList : work){
      singleList.addAll(innerWorkList);
    }

    return singleList;

  }

  private class BlockMapReader extends TimedRunnable<List<CompleteFileWork>> {
    final FileStatus status;

    // This variable blockify indicates if a single file can be read by multiple threads
    // For examples, for CSV, it is set as true
    // because each row in a CSV file can be considered as an independent record;
    // for json, it is set as false
    // because each row in a json file cannot be determined as a record or not simply by that row alone
    final boolean blockify;

    public BlockMapReader(FileStatus status, boolean blockify) {
      super();
      this.status = status;
      this.blockify = blockify;
    }


    @Override
    protected List<CompleteFileWork> runInner() throws Exception {
      final List<CompleteFileWork> work = Lists.newArrayList();
      boolean error = false;
      if (blockify && !compressed(status)) {
        try {
          ImmutableRangeMap<Long, BlockLocation> rangeMap = getBlockMap(status);
          for (Entry<Range<Long>, BlockLocation> l : rangeMap.asMapOfRanges().entrySet()) {
            work.add(new CompleteFileWork(getEndpointByteMap(new FileStatusWork(status, l.getValue().getOffset(), l.getValue().getLength())),
                    l.getValue().getOffset(), l.getValue().getLength(), status));
          }
        } catch (IOException e) {
          logger.warn("failure while generating file work.", e);
          error = true;
        }
      }


      if (!blockify || error || compressed(status)) {
        work.add(new CompleteFileWork(getEndpointByteMap(new FileStatusWork(status)), 0, status.getLen(), status));
      }

      // This if-condition is specific for empty CSV file
      // For CSV files, the global variable blockify is set as true
      // And if this CSV file is empty, rangeMap would be empty also
      // Therefore, at the point before this if-condition, work would not be populated
      if(work.isEmpty()) {
        work.add(new CompleteFileWork(getEndpointByteMap(new FileStatusWork(status)), 0, 0, status));
      }

      return work;
    }


    @Override
    protected IOException convertToIOException(Exception e) {
      return new IOException("Failure while trying to get block map for " + status.getPath(), e);
    }

  }

  private class FileStatusWork implements FileWork{
    private FileStatus status;
    private long start;
    private long length;

    public FileStatusWork(FileStatus status) {
      this(status, 0, status.getLen());
    }

    public FileStatusWork(FileStatus status, long start, long length) {
      Preconditions.checkArgument(!status.isDir(), "FileStatus work only works with files, not directories.");
      this.status = status;
      this.start = start;
      this.length = length;
    }

    @Override
    public FileStatus getStatus() {
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

  /**
   * Builds a mapping of block locations to file byte range
   */
  private ImmutableRangeMap<Long,BlockLocation> buildBlockMap(FileStatus status) throws IOException {
    final Timer.Context context = metrics.timer(BLOCK_MAP_BUILDER_TIMER).time();
    BlockLocation[] blocks;
    ImmutableRangeMap<Long,BlockLocation> blockMap;
    blocks = fs.getFileBlockLocations(status, 0 , status.getLen());
    ImmutableRangeMap.Builder<Long, BlockLocation> blockMapBuilder = new ImmutableRangeMap.Builder<>();
    for (BlockLocation block : blocks) {
      long start = block.getOffset();
      long end = start + block.getLength();
      Range<Long> range = Range.closedOpen(start, end);
      blockMapBuilder = blockMapBuilder.put(range, block);
    }
    blockMap = blockMapBuilder.build();
    blockMapMap.put(status.getPath(), blockMap);
    context.stop();
    return blockMap;
  }

  private ImmutableRangeMap<Long,BlockLocation> getBlockMap(FileStatus status) throws IOException{
    ImmutableRangeMap<Long,BlockLocation> blockMap  = blockMapMap.get(status.getPath());
    if (blockMap == null) {
      blockMap = buildBlockMap(status);
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



    ImmutableRangeMap<Long,BlockLocation> blockMap = getBlockMap(work.getStatus());
    EndpointByteMapImpl endpointByteMap = new EndpointByteMapImpl();
    long start = work.getStart();
    long end = start + work.getLength();
    Range<Long> rowGroupRange = Range.closedOpen(start, end);

    // Find submap of ranges that intersect with the rowGroup
    ImmutableRangeMap<Long,BlockLocation> subRangeMap = blockMap.subRangeMap(rowGroupRange);

    // Iterate through each block in this submap and get the host for the block location
    for (Map.Entry<Range<Long>,BlockLocation> block : subRangeMap.asMapOfRanges().entrySet()) {
      String[] hosts;
      Range<Long> blockRange = block.getKey();
      try {
        hosts = block.getValue().getHosts();
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to get hosts for block location", ioe);
      }
      Range<Long> intersection = rowGroupRange.intersection(blockRange);
      long bytes = intersection.upperEndpoint() - intersection.lowerEndpoint();

      // For each host in the current block location, add the intersecting bytes to the corresponding endpoint
      for (String host : hosts) {
        NodeEndpoint endpoint = getNodeEndpoint(host);
        if (endpoint != null) {
          endpointByteMap.add(endpoint, bytes);
        } else {
          logger.debug("Failure finding SabotNode running on host {}.  Skipping affinity to that host.", host);
        }
      }
    }

    logger.debug("FileWork group ({},{}) max bytes {}", work.getStatus().getPath(), work.getStart(), endpointByteMap.getMaxBytes());

    logger.debug("Took {} ms to set endpoint bytes", watch.stop().elapsed(TimeUnit.MILLISECONDS));
    return endpointByteMap;
  }

  private NodeEndpoint getNodeEndpoint(String hostName) {
    return endPointMap.get(hostName);
  }

  /**
   * Builds a mapping of SabotNode endpoints to hostnames
   */
  private static ImmutableMap<String, NodeEndpoint> buildEndpointMap(Collection<NodeEndpoint> endpoints) {
    Stopwatch watch = Stopwatch.createStarted();
    HashMap<String, NodeEndpoint> endpointMap = Maps.newHashMap();
    for (NodeEndpoint d : endpoints) {
      String hostName = d.getAddress();
      endpointMap.put(hostName, d);
    }
    watch.stop();
    logger.debug("Took {} ms to build endpoint map", watch.elapsed(TimeUnit.MILLISECONDS));
    return ImmutableMap.copyOf(endpointMap);
  }

}
