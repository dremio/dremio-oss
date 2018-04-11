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
package com.dremio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.hadoop.fs.BlockLocation;
import org.junit.Test;

import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.schedule.AssignmentCreator2;
import com.dremio.exec.store.schedule.CompleteWork;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Multimap;
import com.google.common.io.Resources;

public class TestMfsBlockLoader {

  @Test
  public void testAssignments() throws IOException {
    Multimap<String,BlockLocation> blocks = parse("assignments/mfs_block_locations.txt");
    List<MfsWork> workList = getWork(blocks);
    List<NodeEndpoint> endpoints = new ArrayList<>();
    endpoints.addAll(getEndpoints("assignments/allHosts", 12));
    for (int i = 0; i < 1; i++) {
      Multimap<Integer, MfsWork> mappings = AssignmentCreator2.getMappings(endpoints, workList, 1.5);
      verifyMappings(mappings, endpoints);
      Collections.shuffle(endpoints);
    }
  }

  @Test
  public void assignToEmptyListOfNodes() throws IOException {
    try {
      AssignmentCreator2.getMappings(new ArrayList<NodeEndpoint>(), new ArrayList<CompleteWork>(), 1.5);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "No executors available to assign work.");
    }
  }

  private static void verifyMappings(Multimap<Integer,MfsWork> mappings, final List<NodeEndpoint> endpoints) throws FileNotFoundException {
    final AtomicLong localBytes = new AtomicLong(0);
    final AtomicLong totalBytes = new AtomicLong(0);
    List<Long> byteList = FluentIterable.from(mappings.asMap().entrySet())
      .transform(new Function<Entry<Integer,Collection<MfsWork>>, Long>() {
        @Override
        public Long apply(Entry<Integer, Collection<MfsWork>> input) {
          long bytes = 0;
          for (MfsWork work : input.getValue()) {
            long offset = work.getOffset();
            if (offset == 0) {
              for(EndpointAffinity aff : work.getAffinity()){
                long lb = (long) aff.getAffinity();
                bytes += work.totalLength;
                if (lb > 0) {
                  localBytes.addAndGet(work.totalLength);
                  break;
                }

              }
              totalBytes.addAndGet(work.totalLength);
            }
          }
          return bytes;
        }
      }).toSortedList(new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
          return -o1.compareTo(o2);
        }
      });
    double mean = mean(byteList);
    double relativeMax = (double) max(byteList) / mean;
    double localRatio = ((double) localBytes.get()) / totalBytes.get();

    // these conditions are correct for the given block locations at the time of checkin. If changes are made to AssignmentCreator
    // these conditions may need adjustment.
    assertTrue(relativeMax < 1.7);
    assertTrue(localRatio > .75);
  }

  private static long max(List<Long> data) {
    long max = 0;
    for (Long l : data) {
      max = Math.max(max, l);
    }
    return max;
  }

  private static double stdDev(List<Long> data) {
    double[] asDouble = new double[data.size()];
    int i = 0;
    for (Long l : data) {
      asDouble[i++] = (double) l;
    }
    return new StandardDeviation().evaluate(asDouble);
  }

  private static double mean(List<Long> data) {
    double[] asDouble = new double[data.size()];
    int i = 0;
    for (Long l : data) {
      asDouble[i++] = (double) l;
    }
    return new Mean().evaluate(asDouble);
  }

  public static List<NodeEndpoint> getEndpoints(String path, int width) throws IOException {
    BufferedReader reader = getBufferedReaderFromResource(path);
    List<String> hosts = new ArrayList<>();
    String host;
    while ((host = reader.readLine()) != null) {
      hosts.add(host);
    }
    List<String> fullList = new ArrayList<>();
    for (int i = 0; i < width; i++) {
      fullList.addAll(hosts);
    }
    return FluentIterable.from(fullList).transform(new Function<String, NodeEndpoint>() {
      @Override
      public NodeEndpoint apply(String host) {
        return getEndpoint(host);
      }
    }).toList();
  }

  private static NodeEndpoint getEndpoint(String host) {
    return NodeEndpoint.newBuilder().setAddress(host).setFabricPort(1234).build();
  }

  public static List<MfsWork> getWork(Multimap<String,BlockLocation> blockLocations) throws IOException {
    List<MfsWork> workList = new ArrayList<>();
    for (String file : blockLocations.keySet()) {
      Collection<BlockLocation> blocks = blockLocations.get(file);
      long totalLength = 0;
      for (BlockLocation block : blocks) {
        totalLength += block.getLength();
      }
      for (BlockLocation block : blocks) {
        List<EndpointAffinity> affinityList = new ArrayList<>();
        for (String host : block.getHosts()) {
          NodeEndpoint nodeEndpoint = getEndpoint(host);
          affinityList.add(new EndpointAffinity(nodeEndpoint, block.getLength()));
        }
        MfsWork work = new MfsWork(block.getOffset(), block.getLength(), totalLength, affinityList);
        workList.add(work);
      }
    }
    return workList;
  }

  private static BufferedReader getBufferedReaderFromResource(String path) throws IOException {
    InputStream stream = new ByteArrayInputStream(Resources.toByteArray(Resources.getResource(path)));
    return new BufferedReader(new InputStreamReader(stream));
  }

  public static Multimap<String,BlockLocation> parse(String path) throws IOException {
    BufferedReader reader = getBufferedReaderFromResource(path);
    Multimap<String,BlockLocation> blockLocations = ArrayListMultimap.create();
    while (true) {
      String fileLine = reader.readLine();
      log(fileLine);
      if (fileLine == null) {
        break;
      }
      String[] fields = fileLine.split("\\s+");
      String filePath = fields[11];
      long length = Integer.parseInt(fields[7]);
      long blockSize = Integer.parseInt(fields[10]);
      long numBlocks = length < 64 * 1024 ? 0 : (long) Math.ceil((float) length / blockSize);
      String primaryBlockLine = reader.readLine();
      log(primaryBlockLine);
      if (numBlocks == 0) {
        String[] hosts = getHosts(primaryBlockLine);
        BlockLocation location = new BlockLocation(hosts, hosts, 0, length);
        blockLocations.put(filePath, location);
        continue;
      }
      for (int i = 0; i < numBlocks; i++) {
        String blockLine = reader.readLine();
        log(blockLine);
        String[] hosts = getHosts(blockLine);
        long offset = i * blockSize;
        long currentBlockSize = Math.min(length - offset, blockSize);
        BlockLocation location = new BlockLocation(hosts, hosts, offset, currentBlockSize);
        blockLocations.put(filePath, location);
      }
    }
    return blockLocations;
  }

  private static String[] getHosts(String blockLine) {
    String[] primaryBlockLineFields = blockLine.split("\\s+");
    String[] hostsWithPort = Arrays.copyOfRange(primaryBlockLineFields, 3, primaryBlockLineFields.length);
    String[] hosts = new String[hostsWithPort.length];
    int i = 0;
    for (String hostWithPort : hostsWithPort) {
      hosts[i++] = hostWithPort.split(":")[0];
    }
    return hosts;
  }

  private static void log(Object s) {
//    System.out.println(s);
  }

  private static class MfsWork implements CompleteWork {
    private long offset;
    private long length;
    private long totalLength;
    private final List<EndpointAffinity> affinities;

    private MfsWork(long offset, long length, long totalLength, List<EndpointAffinity> affinities) {
      this.offset = offset;
      this.length = length;
      this.totalLength = totalLength;
      this.affinities = affinities;
    }

    public long getOffset() {
      return offset;
    }

    @Override
    public long getTotalBytes() {
      return length;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return Long.compare(getTotalBytes(), o.getTotalBytes());
    }

    @Override
    public List<EndpointAffinity> getAffinity() {
      return affinities;
    }
  }
}

