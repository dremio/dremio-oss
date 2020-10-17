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
/*
 * This file is copy of impl/RecordReaderUtils.java from org/apache/orc project
 * It sets Arrow's BufferAllocator for ByteBuffer allocations
 * These ByteBuffers are used in zero copy read path of Hive ORC tables
 */
package com.dremio.exec.store.hive.exec;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;


import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ByteBufferUtil;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.DirectDecompressionCodec;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.HadoopShimsFactory;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

/**
 * Stateless methods shared between RecordReaderImpl and EncodedReaderImpl.
 */
public class DremioORCRecordUtils {
  private static final HadoopShims SHIMS = HadoopShimsFactory.get();
  private static final Logger LOG = LoggerFactory.getLogger(DremioORCRecordUtils.class);

  static boolean hadBadBloomFilters(TypeDescription.Category category,
                                    OrcFile.WriterVersion version) {
    switch(category) {
      case STRING:
      case CHAR:
      case VARCHAR:
        return !version.includes(OrcFile.WriterVersion.HIVE_12055);
      case DECIMAL:
        return true;
      case TIMESTAMP:
        return !version.includes(OrcFile.WriterVersion.ORC_135);
      default:
        return false;
    }
  }

  /**
   * Plans the list of disk ranges that the given stripe needs to read the
   * indexes. All of the positions are relative to the start of the stripe.
   * @param  fileSchema the schema for the file
   * @param footer the stripe footer
   * @param ignoreNonUtf8BloomFilter should the reader ignore non-utf8
   *                                 encoded bloom filters
   * @param fileIncluded the columns (indexed by file columns) that should be
   *                     read
   * @param sargColumns true for the columns (indexed by file columns) that
   *                    we need bloom filters for
   * @param version the version of the software that wrote the file
   * @param bloomFilterKinds (output) the stream kind of the bloom filters
   * @return a list of merged disk ranges to read
   */
  public static DiskRangeList planIndexReading(TypeDescription fileSchema,
                                               OrcProto.StripeFooter footer,
                                               boolean ignoreNonUtf8BloomFilter,
                                               boolean[] fileIncluded,
                                               boolean[] sargColumns,
                                               OrcFile.WriterVersion version,
                                               OrcProto.Stream.Kind[] bloomFilterKinds) {
    DiskRangeList.CreateHelper result = new DiskRangeList.CreateHelper();
    List<OrcProto.Stream> streams = footer.getStreamsList();
    // figure out which kind of bloom filter we want for each column
    // picks bloom_filter_utf8 if its available, otherwise bloom_filter
    if (sargColumns != null) {
      for (OrcProto.Stream stream : streams) {
        if (stream.hasKind() && stream.hasColumn()) {
          int column = stream.getColumn();
          if (sargColumns[column]) {
            switch (stream.getKind()) {
              case BLOOM_FILTER:
                if (bloomFilterKinds[column] == null &&
                  !(ignoreNonUtf8BloomFilter &&
                    hadBadBloomFilters(fileSchema.findSubtype(column)
                      .getCategory(), version))) {
                  bloomFilterKinds[column] = OrcProto.Stream.Kind.BLOOM_FILTER;
                }
                break;
              case BLOOM_FILTER_UTF8:
                bloomFilterKinds[column] = OrcProto.Stream.Kind.BLOOM_FILTER_UTF8;
                break;
              default:
                break;
            }
          }
        }
      }
    }
    long offset = 0;
    for(OrcProto.Stream stream: footer.getStreamsList()) {
      if (stream.hasKind() && stream.hasColumn()) {
        int column = stream.getColumn();
        if (fileIncluded == null || fileIncluded[column]) {
          boolean needStream = false;
          switch (stream.getKind()) {
            case ROW_INDEX:
              needStream = true;
              break;
            case BLOOM_FILTER:
              needStream = bloomFilterKinds[column] == OrcProto.Stream.Kind.BLOOM_FILTER;
              break;
            case BLOOM_FILTER_UTF8:
              needStream = bloomFilterKinds[column] == OrcProto.Stream.Kind.BLOOM_FILTER_UTF8;
              break;
            default:
              // PASS
              break;
          }
          if (needStream) {
            result.addOrMerge(offset, offset + stream.getLength(), true, false);
          }
        }
      }
      offset += stream.getLength();
    }
    return result.get();
  }

  public static class DefaultDataReader implements DataReader {
    protected final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
    private FSDataInputStream file = null;
    private DremioORCRecordUtils.ByteBufferAllocatorPool pool;
    private HadoopShims.ZeroCopyReaderShim zcr = null;
    private final FileSystem fs;
    private final Path path;
    private final boolean useZeroCopy;
    private CompressionCodec codec;
    private final int bufferSize;
    private final int typeCount;
    private CompressionKind compressionKind;
    private final BufferAllocator allocator;
    private boolean useDirectMemory = true;
    private final boolean doComputeLocality;
    private boolean remoteRead = false;
    private final Set<ByteBuffer> buffersToRelease = Sets.newIdentityHashSet();
    private final Set<ByteBuffer> directBuffersToRelease = Sets.newIdentityHashSet();

    private DefaultDataReader(BufferAllocator allocator, DataReaderProperties properties, boolean useDirectMemory,
                              final boolean doComputeLocality) {
      this.fs = properties.getFileSystem();
      this.path = properties.getPath();
      this.useZeroCopy = properties.getZeroCopy();
      this.compressionKind = properties.getCompression();
      this.codec = OrcCodecPool.getCodec(compressionKind);
      this.bufferSize = properties.getBufferSize();
      this.typeCount = properties.getTypeCount();
      this.pool = new DremioORCRecordUtils.ByteBufferAllocatorPool(allocator);
      this.allocator = allocator;
      this.useDirectMemory = useDirectMemory;
      this.doComputeLocality = doComputeLocality;
    }

    @Override
    public void open() throws IOException {
      this.file = fs.open(path);
      if (useDirectMemory) {
        useDirectMemory = DremioORCRecordUtils.directReadAllowed(codec);
      }
      if (useDirectMemory && useZeroCopy) {
        zcr = DremioORCRecordUtils.createZeroCopyShim(file, codec, pool);
      } else {
        zcr = null;
      }
    }

    static List<DiskRange> singleton(DiskRange item) {
      List<DiskRange> result = new ArrayList();
      result.add(item);
      return result;
    }

    @Override
    public OrcIndex readRowIndex(StripeInformation stripe,
                                 TypeDescription fileSchema,
                                 OrcProto.StripeFooter footer,
                                 boolean ignoreNonUtf8BloomFilter,
                                 boolean[] included,
                                 OrcProto.RowIndex[] indexes,
                                 boolean[] sargColumns,
                                 OrcFile.WriterVersion version,
                                 OrcProto.Stream.Kind[] bloomFilterKinds,
                                 OrcProto.BloomFilterIndex[] bloomFilterIndices
    ) throws IOException {
      if (file == null) {
        open();
      }
      if (footer == null) {
        footer = readStripeFooter(stripe);
      }
      if (indexes == null) {
        indexes = new OrcProto.RowIndex[typeCount];
      }
      if (bloomFilterKinds == null) {
        bloomFilterKinds = new OrcProto.Stream.Kind[typeCount];
      }
      if (bloomFilterIndices == null) {
        bloomFilterIndices = new OrcProto.BloomFilterIndex[typeCount];
      }
      DiskRangeList ranges = planIndexReading(fileSchema, footer,
        ignoreNonUtf8BloomFilter, included, sargColumns, version,
        bloomFilterKinds);
      ranges = readFileData(ranges, stripe.getOffset(), false);
      long offset = 0;
      DiskRangeList range = ranges;
      for(OrcProto.Stream stream: footer.getStreamsList()) {
        // advance to find the next range
        while (range != null && range.getEnd() <= offset) {
          range = range.next;
        }
        // no more ranges, so we are done
        if (range == null) {
          break;
        }
        int column = stream.getColumn();
        if (stream.hasKind() && range.getOffset() <= offset) {
          switch (stream.getKind()) {
            case ROW_INDEX:
              if (included == null || included[column]) {
                ByteBuffer bb = range.getData().duplicate();
                bb.position((int) (bb.position() + offset - range.getOffset()));
                bb.limit((int) (bb.position() + stream.getLength()));
                indexes[column] = OrcProto.RowIndex.parseFrom(
                  InStream.createCodedInputStream("index",
                    singleton(new BufferChunk(bb, 0)),
                    stream.getLength(), codec, bufferSize));
              }
              break;
            case BLOOM_FILTER:
            case BLOOM_FILTER_UTF8:
              if (sargColumns != null && sargColumns[column]) {
                ByteBuffer bb = range.getData().duplicate();
                bb.position((int) (bb.position() + offset - range.getOffset()));
                bb.limit((int) (bb.position() + stream.getLength()));
                bloomFilterIndices[column] = OrcProto.BloomFilterIndex.parseFrom
                  (InStream.createCodedInputStream("bloom_filter",
                    singleton(new BufferChunk(bb, 0)),
                    stream.getLength(), codec, bufferSize));
              }
              break;
            default:
              break;
          }
        }
        offset += stream.getLength();
      }
      return new OrcIndex(indexes, bloomFilterKinds, bloomFilterIndices);
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
      if (file == null) {
        open();
      }
      long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
      int tailLength = (int) stripe.getFooterLength();

      // read the footer
      ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
      file.readFully(offset, tailBuf.array(), tailBuf.arrayOffset(), tailLength);
      return OrcProto.StripeFooter.parseFrom(
        InStream.createCodedInputStream("footer", singleton(
          new BufferChunk(tailBuf, 0)), tailLength, codec, bufferSize));
    }

    @Override
    public DiskRangeList readFileData(
      DiskRangeList range, long baseOffset, boolean doForceDirect) throws IOException {
      // if zero copy is set, then try reading using zero copy first
      if (zcr != null) {
        try {
          return readDiskRangesUsingZCR(fs, file, path, zcr, pool, baseOffset, range);
        }
        catch (UnsupportedOperationException ioe) {
          // zero copy read failed. Clear all buffers and unset zero copy read
          if (pool != null) {
            pool.clear();
          }
          zcr.close();
          zcr = null;
          logger.warn("Zero copy read failed for path: {}. Using fallback read path", path);
        }
      }

      // if read can be done using direct memory, then try reading using direct memory
      if (useDirectMemory) {
        return readDiskRangesUsingDirectMemory(fs, file, path, pool, baseOffset, range);
      }

      // read using heap memory
      return readDiskRangesUsingHeapMemory(fs, file, path, baseOffset, range);
    }

    @Override
    public void close() throws IOException {

      for (ByteBuffer buffer : buffersToRelease) {
        releaseBufferWithoutTracking(buffer);
      }
      buffersToRelease.clear();

      for (ByteBuffer buffer : directBuffersToRelease) {
        pool.putBuffer(buffer);
      }
      directBuffersToRelease.clear();

      if (codec != null) {
        OrcCodecPool.returnCodec(compressionKind, codec);
        codec = null;
      }
      if (pool != null) {
        pool.clear();
        pool = null;
      }
      // close both zcr and file
      try (HadoopShims.ZeroCopyReaderShim myZcr = zcr) {
        if (file != null) {
          file.close();
          file = null;
          zcr = null;
        }
      }
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return useDirectMemory;
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      if (directBuffersToRelease.remove(buffer)) {
        pool.putBuffer(buffer);
        return;
      }
      buffersToRelease.remove(buffer);
      releaseBufferWithoutTracking(buffer);
    }

    private void releaseBufferWithoutTracking(ByteBuffer buffer) {
      if (zcr != null) {
        zcr.releaseBuffer(buffer);
      } else {
        pool.putBuffer(buffer);
      }
    }

    @Override
    public HadoopShims.ByteBufferPoolShim getBufferPool() {
      return pool;
    }

    @Override
    public DataReader clone() {
      if (this.file != null) {
        // We should really throw here, but that will cause failures in Hive.
        // While Hive uses clone, just log a warning.
        LOG.warn("Cloning an opened DataReader; the stream will be reused and closed twice");
      }
      try {
        DremioORCRecordUtils.DefaultDataReader clone = (DremioORCRecordUtils.DefaultDataReader) super.clone();
        if (codec != null) {
          // Make sure we don't share the same codec between two readers.
          clone.codec = OrcCodecPool.getCodec(clone.compressionKind);
        }
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new UnsupportedOperationException("uncloneable", e);
      }
    }

    @Override
    public CompressionCodec getCompressionCodec() {
      return codec;
    }

    private DiskRangeList readDiskRangesUsingZCR(FileSystem fs, FSDataInputStream file,
                                                 Path path, HadoopShims.ZeroCopyReaderShim zcr,
                                                 ByteBufferAllocatorPool pool, long base, DiskRangeList range) throws IOException {
      return readDiskRanges(fs, file, path, zcr, pool, true, base, range);
    }


    private DiskRangeList readDiskRangesUsingDirectMemory(FileSystem fs, FSDataInputStream file,
                                                          Path path, ByteBufferAllocatorPool pool, long base, DiskRangeList range) throws IOException {
      return readDiskRanges(fs, file, path, null, pool, true, base, range);
    }

    private DiskRangeList readDiskRangesUsingHeapMemory(FileSystem fs, FSDataInputStream file,
                                                        Path path, long base, DiskRangeList range
    ) throws IOException {
      return readDiskRanges(fs, file, path, null, null, false, base, range);
    }

    private void computeLocality(FileSystem fs, Path path, DiskRangeList range) {
      if (this.remoteRead) {
        return;
      }

      boolean currentReadIsRemote = false;
      try {
        String localHost = InetAddress.getLocalHost().getCanonicalHostName();
        while (range != null) {
          int len = (int) (range.getEnd() - range.getOffset());
          long off = range.getOffset();
          BlockLocation[] blockLocations = fs.getFileBlockLocations(path, off, len);
          List<Range<Long>> intersectingRanges = new ArrayList<>();
          Range<Long> rowGroupRange = Range.openClosed(off, off+len);
          for (BlockLocation loc : blockLocations) {
            for (String host : loc.getHosts()) {
              if (host.equals(localHost)) {
                intersectingRanges.add(Range.closedOpen(loc.getOffset(), loc.getOffset() + loc.getLength()).intersection(rowGroupRange));
              }
            }
          }
          long totalIntersect = 0;
          for (Range<Long> intersectingRange : intersectingRanges) {
            totalIntersect += (intersectingRange.upperEndpoint() - intersectingRange.lowerEndpoint());
          }
          if (totalIntersect < len) {
            currentReadIsRemote = true;
            break;
          }
          range = range.next;
        }
      } catch (Throwable e) {
        // ignoring any exception in this code path as it is used to collect
        // remote readers metric in profile for debugging
        logger.debug("computeLocality failed with message: {} for path {}", e.getMessage(), path, e);
      }

      if (currentReadIsRemote) {
        this.remoteRead = true;
      }
    }

    /**
     * Read the list of ranges from the file.
     * @param fs FileSystem object to get block locations of the file
     * @param file the file to read
     * @param base the base of the stripe
     * @param range the disk ranges within the stripe to read
     * @return the bytes read for each disk range, which is the same length as
     *    ranges
     * @throws IOException
     */
    private  DiskRangeList readDiskRanges(FileSystem fs, FSDataInputStream file,
                                          Path path, HadoopShims.ZeroCopyReaderShim zcr, ByteBufferAllocatorPool pool, boolean useDirectMemory, long base, DiskRangeList range) throws IOException {
      if (range == null) {
        return null;
      }
      if (doComputeLocality) {
        computeLocality(fs, path, range);
      }
      DiskRangeList prev = range.prev;
      if (prev == null) {
        prev = new DiskRangeList.MutateHelper(range);
      }
      while (range != null) {
        if (range.hasData()) {
          range = range.next;
          continue;
        }
        int len = (int) (range.getEnd() - range.getOffset());
        long off = range.getOffset();
        if (useDirectMemory) {
          file.seek(base + off);
          List<ByteBuffer> bytes = new ArrayList<>();
          while (len > 0) {
            ByteBuffer partial;
            if (zcr != null) {
              partial = zcr.readBuffer(len, false);
            } else {
              // in the zero copy read path, when memory mapped file does not exist,
              // hadoop client uses following call to read using direct memory
              partial = ByteBufferUtil.fallbackRead(file, pool, len);
            }
            bytes.add(partial);
            buffersToRelease.add(partial);
            int read = partial.remaining();
            len -= read;
            off += read;
          }

          ByteBuffer chunkBuffer;
          long chunkOffset = range.getOffset();
          int chunkLength = (int) (range.getEnd() - range.getOffset());
          if (bytes.size() > 1) {
            if (pool != null) {
              chunkBuffer = pool.getBuffer(true, chunkLength);
              directBuffersToRelease.add(chunkBuffer);
            } else {
              byte[] buffer = new byte[chunkLength];
              chunkBuffer = ByteBuffer.wrap(buffer);
            }
            for (ByteBuffer byteBuffer : bytes) {
              chunkBuffer.put(byteBuffer.duplicate());
              releaseBuffer(byteBuffer);
            }
            chunkBuffer.flip();
          } else {
            Preconditions.checkState(bytes.size() == 1, "Empty bytes found");
            chunkBuffer = bytes.get(0);
          }
          BufferChunk bc = new BufferChunk(chunkBuffer, chunkOffset);
          range.replaceSelfWith(bc);
          range = bc;
        } else {
          byte[] buffer = new byte[len];
          file.readFully((base + off), buffer, 0, buffer.length);
          ByteBuffer bb = ByteBuffer.wrap(buffer);
          range = range.replaceSelfWith(new BufferChunk(bb, range.getOffset()));
        }
        range = range.next;
      }
      return prev.next;
    }

    public boolean isRemoteRead() {
      return this.remoteRead;
    }
  }

  public static DremioORCRecordUtils.DefaultDataReader createDefaultDataReader(BufferAllocator allocator, DataReaderProperties properties,
                                                                               boolean useDirectMemory, final boolean doComputeLocality) {
    return new DremioORCRecordUtils.DefaultDataReader(allocator, properties, useDirectMemory, doComputeLocality);
  }

  /*
   * check if file read can be done using direct memory or not
   */
  static boolean directReadAllowed(CompressionCodec codec) {
    if ((codec == null || ((codec instanceof DirectDecompressionCodec)
      && ((DirectDecompressionCodec) codec).isAvailable()))) {
      return true;
    }
    return false;
  }

  static HadoopShims.ZeroCopyReaderShim createZeroCopyShim(FSDataInputStream file,
                                                           CompressionCodec codec, DremioORCRecordUtils.ByteBufferAllocatorPool pool) throws IOException {
    if (directReadAllowed(codec)) {
      /* codec is null or is available */
      return SHIMS.getZeroCopyReader(file, pool);
    }
    return null;
  }

  // this is an implementation copied from ElasticByteBufferPool in hadoop-2,
  // which lacks a clear()/clean() operation
  public final static class ByteBufferAllocatorPool implements HadoopShims.ByteBufferPoolShim, ByteBufferPool {
    private BufferAllocator allocator;
    ByteBufferAllocatorPool(BufferAllocator allocator) {
      this.allocator = allocator;
    }
    private static final class Key implements Comparable<DremioORCRecordUtils.ByteBufferAllocatorPool.Key> {
      private final int capacity;
      private final long insertionGeneration;

      Key(int capacity, long insertionGeneration) {
        this.capacity = capacity;
        this.insertionGeneration = insertionGeneration;
      }

      @Override
      public int compareTo(DremioORCRecordUtils.ByteBufferAllocatorPool.Key other) {
        if (capacity != other.capacity) {
          return capacity - other.capacity;
        } else {
          return Long.compare(insertionGeneration, other.insertionGeneration);
        }
      }

      @Override
      public boolean equals(Object rhs) {
        if (rhs == null) {
          return false;
        }
        try {
          DremioORCRecordUtils.ByteBufferAllocatorPool.Key o = (DremioORCRecordUtils.ByteBufferAllocatorPool.Key) rhs;
          return (compareTo(o) == 0);
        } catch (ClassCastException e) {
          return false;
        }
      }

      @Override
      public int hashCode() {
        int iConstant = 37;
        int iTotal = 17;
        iTotal = iTotal * iConstant + capacity;
        iTotal = iTotal * iConstant + (int)(insertionGeneration ^ insertionGeneration >> 32);
        return iTotal;
      }
    }
    private static final class ByteBufferWrapper {
      private final ByteBuffer byteBuffer;

      ByteBufferWrapper(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
      }

      public boolean equals(Object rhs) {
        return (rhs instanceof ByteBufferWrapper) && (this.byteBuffer == ((ByteBufferWrapper) rhs).byteBuffer);
      }

      public int hashCode() {
        return System.identityHashCode(byteBuffer);
      }
    }
    private final Map<ByteBufferWrapper, ArrowBuf> directBufMap = new HashMap<>();
    private final TreeMap<DremioORCRecordUtils.ByteBufferAllocatorPool.Key, ByteBuffer> buffers = new TreeMap<DremioORCRecordUtils.ByteBufferAllocatorPool.Key, ByteBuffer>();

    private long currentGeneration = 0;

    private final TreeMap<DremioORCRecordUtils.ByteBufferAllocatorPool.Key, ByteBuffer> getBufferTree() {
      return buffers;
    }

    public void clear() {
      buffers.clear();
      for (ArrowBuf buf : directBufMap.values()) {
        buf.release();
      }
      directBufMap.clear();
    }

    @Override
    public ByteBuffer getBuffer(boolean direct, int length) {
      if (direct) {
        ArrowBuf buf = allocator.buffer(length);
        ByteBuffer retBuf = buf.nioBuffer(0, length);
        directBufMap.put(new ByteBufferWrapper(retBuf), buf);
        return retBuf;
      } else {
        TreeMap<DremioORCRecordUtils.ByteBufferAllocatorPool.Key, ByteBuffer> tree = getBufferTree();
        Map.Entry<DremioORCRecordUtils.ByteBufferAllocatorPool.Key, ByteBuffer> entry = tree.ceilingEntry(new DremioORCRecordUtils.ByteBufferAllocatorPool.Key(length, 0));
        if (entry == null) {
          return ByteBuffer.allocate(length);
        }
        tree.remove(entry.getKey());
        return entry.getValue();
      }
    }

    @Override
    public void putBuffer(ByteBuffer buffer) {
      if (buffer.isDirect()) {
        ArrowBuf buf = directBufMap.remove(new ByteBufferWrapper(buffer));
        if (buf != null) {
          buf.release();
        }
      } else {
        TreeMap<DremioORCRecordUtils.ByteBufferAllocatorPool.Key, ByteBuffer> tree = getBufferTree();
        while (true) {
          DremioORCRecordUtils.ByteBufferAllocatorPool.Key key = new DremioORCRecordUtils.ByteBufferAllocatorPool.Key(buffer.capacity(), currentGeneration++);
          if (!tree.containsKey(key)) {
            tree.put(key, buffer);
            return;
          }
          // Buffers are indexed by (capacity, generation).
          // If our key is not unique on the first try, we try again
        }
      }
    }
  }
}
