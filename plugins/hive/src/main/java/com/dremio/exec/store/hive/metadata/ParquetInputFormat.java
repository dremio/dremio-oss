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
package com.dremio.exec.store.hive.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StopWatch;

/**
 * This file contains getSplits implementation for Hive parquet datasets.
 * Most of the code is taken form Apache Hadoop FileInputFormat.java file of 3.2.1 version
 */
public class ParquetInputFormat extends FileInputFormat<NullWritable, ArrayWritable> {

  /**
   * Represents a parquet file split. In addition to FileSplit attributes, it contains
   * file length and last modification time
   */
  public static class ParquetSplit extends FileSplit {

    private final long modificationTime;
    private final long fileSize;

    public ParquetSplit() {
      super();
      modificationTime = 0;
      fileSize = 0;
    }

    public ParquetSplit(Path file, long start, long length, String[] hosts, long fileSize, long modificationTime) {
      super(file, start, length, hosts);
      this.modificationTime = modificationTime;
      this.fileSize = fileSize;
    }

    public ParquetSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts, long fileSize, long modificationTime) {
      super(file, start, length, hosts, inMemoryHosts);
      this.modificationTime = modificationTime;
      this.fileSize = fileSize;
    }

    public long getModificationTime() {
      return modificationTime;
    }

    public long getFileSize() {
      return fileSize;
    }
  }

  private FileSplit makeParquetSplit(Path file, long start, long length,
                                     String[] hosts,
                                     long fileSize, long modificationTime) {
    return new ParquetSplit(file, start, length, hosts, fileSize, modificationTime);
  }

  private FileSplit makeParquetSplit(Path file, long start, long length,
                                     String[] hosts, String[] inMemoryHosts,
                                     long fileSize, long modificationTime) {
    return new ParquetSplit(file, start, length, hosts, inMemoryHosts, fileSize, modificationTime);
  }

  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    StopWatch sw = new StopWatch().start();
    FileStatus[] stats = listStatus(job);

    // Save the number of input files for metrics/loadgen
    job.setLong(NUM_INPUT_FILES, stats.length);
    long totalSize = 0;                           // compute total size
    boolean ignoreDirs = !job.getBoolean(INPUT_DIR_RECURSIVE, false)
      && job.getBoolean(INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, false);

    List<FileStatus> files = new ArrayList<>(stats.length);
    for (FileStatus file: stats) {                // check we have valid files
      if (file.isDirectory()) {
        if (!ignoreDirs) {
          throw new IOException("Not a file: "+ file.getPath());
        }
      } else {
        files.add(file);
        totalSize += file.getLen();
      }
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      long modificationTime = file.getModificationTime();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(fs, path)) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
              length-bytesRemaining, splitSize, clusterMap);
            splits.add(makeParquetSplit(path, length-bytesRemaining, splitSize,
              splitHosts[0], splitHosts[1], length, modificationTime));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
              - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeParquetSplit(path, length - bytesRemaining, bytesRemaining,
              splitHosts[0], splitHosts[1], length, modificationTime));
          }
        } else {
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
          splits.add(makeParquetSplit(path, 0, length, splitHosts[0], splitHosts[1], length, modificationTime));
        }
      } else {
        //Create empty hosts array for zero length files
        splits.add(makeParquetSplit(path, 0, length, new String[0], length, modificationTime));
      }
    }
    sw.stop();
    return splits.toArray(new FileSplit[splits.size()]);
  }


  // Code below is untouched from original FileInputSplit.java.
  // Copied here since these are private in original source

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
    throw new UnsupportedOperationException();
  }

  private static final double SPLIT_SLOP = 1.1;   // 10% slop
  private long minSplitSize = 1;
  public static final String INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS =
    "mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs";

  /**
   * This function identifies and returns the hosts that contribute
   * most for a given split. For calculating the contribution, rack
   * locality is treated on par with host locality, so hosts from racks
   * that contribute the most are preferred over hosts on racks that
   * contribute less
   * @param blkLocations The list of block locations
   * @param offset
   * @param splitSize
   * @return two arrays - one of hosts that contribute most to this split, and
   *    one of hosts that contribute most to this split that have the data
   *    cached on them
   * @throws IOException
   */
  private String[][] getSplitHostsAndCachedHosts(BlockLocation[] blkLocations,
                                                 long offset, long splitSize, NetworkTopology clusterMap)
    throws IOException {

    int startIndex = getBlockIndex(blkLocations, offset);

    long bytesInThisBlock = blkLocations[startIndex].getOffset() +
      blkLocations[startIndex].getLength() - offset;

    //If this is the only block, just return
    if (bytesInThisBlock >= splitSize) {
      return new String[][] { blkLocations[startIndex].getHosts(),
        blkLocations[startIndex].getCachedHosts() };
    }

    long bytesInFirstBlock = bytesInThisBlock;
    int index = startIndex + 1;
    splitSize -= bytesInThisBlock;

    while (splitSize > 0) {
      bytesInThisBlock =
        Math.min(splitSize, blkLocations[index++].getLength());
      splitSize -= bytesInThisBlock;
    }

    long bytesInLastBlock = bytesInThisBlock;
    int endIndex = index - 1;

    Map<Node,NodeInfo> hostsMap = new IdentityHashMap<Node,NodeInfo>();
    Map <Node,NodeInfo> racksMap = new IdentityHashMap<Node,NodeInfo>();
    String [] allTopos = new String[0];

    // Build the hierarchy and aggregate the contribution of
    // bytes at each level. See TestGetSplitHosts.java

    for (index = startIndex; index <= endIndex; index++) {

      // Establish the bytes in this block
      if (index == startIndex) {
        bytesInThisBlock = bytesInFirstBlock;
      }
      else if (index == endIndex) {
        bytesInThisBlock = bytesInLastBlock;
      }
      else {
        bytesInThisBlock = blkLocations[index].getLength();
      }

      allTopos = blkLocations[index].getTopologyPaths();

      // If no topology information is available, just
      // prefix a fakeRack
      if (allTopos.length == 0) {
        allTopos = fakeRacks(blkLocations, index);
      }

      // NOTE: This code currently works only for one level of
      // hierarchy (rack/host). However, it is relatively easy
      // to extend this to support aggregation at different
      // levels

      for (String topo: allTopos) {

        Node node, parentNode;
        NodeInfo nodeInfo, parentNodeInfo;

        node = clusterMap.getNode(topo);

        if (node == null) {
          node = new NodeBase(topo);
          clusterMap.add(node);
        }

        nodeInfo = hostsMap.get(node);

        if (nodeInfo == null) {
          nodeInfo = new NodeInfo(node);
          hostsMap.put(node,nodeInfo);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
          if (parentNodeInfo == null) {
            parentNodeInfo = new NodeInfo(parentNode);
            racksMap.put(parentNode,parentNodeInfo);
          }
          parentNodeInfo.addLeaf(nodeInfo);
        }
        else {
          nodeInfo = hostsMap.get(node);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
        }

        nodeInfo.addValue(index, bytesInThisBlock);
        parentNodeInfo.addValue(index, bytesInThisBlock);

      } // for all topos

    } // for all indices

    // We don't yet support cached hosts when bytesInThisBlock > splitSize
    return new String[][] { identifyHosts(allTopos.length, racksMap),
      new String[0]};
  }

  private String[] identifyHosts(int replicationFactor,
                                 Map<Node,NodeInfo> racksMap) {

    String [] retVal = new String[replicationFactor];

    List <NodeInfo> rackList = new LinkedList<NodeInfo>();

    rackList.addAll(racksMap.values());

    // Sort the racks based on their contribution to this split
    sortInDescendingOrder(rackList);

    boolean done = false;
    int index = 0;

    // Get the host list for all our aggregated items, sort
    // them and return the top entries
    for (NodeInfo ni: rackList) {

      Set<NodeInfo> hostSet = ni.getLeaves();

      List<NodeInfo>hostList = new LinkedList<NodeInfo>();
      hostList.addAll(hostSet);

      // Sort the hosts in this rack based on their contribution
      sortInDescendingOrder(hostList);

      for (NodeInfo host: hostList) {
        // Strip out the port number from the host name
        retVal[index++] = host.node.getName().split(":")[0];
        if (index == replicationFactor) {
          done = true;
          break;
        }
      }

      if (done) {
        break;
      }
    }
    return retVal;
  }

  private String[] fakeRacks(BlockLocation[] blkLocations, int index)
    throws IOException {
    String[] allHosts = blkLocations[index].getHosts();
    String[] allTopos = new String[allHosts.length];
    for (int i = 0; i < allHosts.length; i++) {
      allTopos[i] = NetworkTopology.DEFAULT_RACK + "/" + allHosts[i];
    }
    return allTopos;
  }

  private static class NodeInfo {
    final Node node;
    final Set<Integer> blockIds;
    final Set<ParquetInputFormat.NodeInfo> leaves;

    private long value;

    NodeInfo(Node node) {
      this.node = node;
      blockIds = new HashSet<Integer>();
      leaves = new HashSet<ParquetInputFormat.NodeInfo>();
    }

    long getValue() {return value;}

    void addValue(int blockIndex, long value) {
      if (blockIds.add(blockIndex)) {
        this.value += value;
      }
    }

    Set<ParquetInputFormat.NodeInfo> getLeaves() { return leaves;}

    void addLeaf(ParquetInputFormat.NodeInfo nodeInfo) {
      leaves.add(nodeInfo);
    }
  }

  private void sortInDescendingOrder(List<ParquetInputFormat.NodeInfo> mylist) {
    Collections.sort(mylist, new Comparator<ParquetInputFormat.NodeInfo>() {
        public int compare(ParquetInputFormat.NodeInfo obj1, ParquetInputFormat.NodeInfo obj2) {

          if (obj1 == null || obj2 == null) {
            return -1;
          }

          if (obj1.getValue() == obj2.getValue()) {
            return 0;
          }
          else {
            return ((obj1.getValue() < obj2.getValue()) ? 1 : -1);
          }
        }
      }
    );
  }

}
