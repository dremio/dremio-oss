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
package com.dremio.sabot.op.receiver;

import static com.dremio.exec.cache.VectorAccessibleSerializable.readIntoArrowBuf;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.dremio.common.AutoCloseables;
import com.dremio.common.DeferredException;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentRecordBatch;
import com.dremio.exec.store.LocalSyncableFileSystem;
import com.dremio.sabot.exec.fragment.FragmentWorkQueue;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.dremio.service.spill.SpillService;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Queues;

/**
 * This implementation of RawBatchBuffer starts writing incoming batches to disk once the buffer size reaches a threshold.
 * The order of the incoming buffers is maintained.
 */
public class SpoolingRawBatchBuffer extends BaseRawBatchBuffer<SpoolingRawBatchBuffer.RawFragmentBatchWrapper> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpoolingRawBatchBuffer.class);

  private static final String DREMIO_LOCAL_IMPL_STRING = "fs.dremio-local.impl";
  private static final Configuration SPOOLING_CONFIG;

  static {
    SPOOLING_CONFIG = new Configuration();
    SPOOLING_CONFIG.set(DREMIO_LOCAL_IMPL_STRING, LocalSyncableFileSystem.class.getName());
    SPOOLING_CONFIG.set("fs.file.impl", LocalSyncableFileSystem.class.getName());
    SPOOLING_CONFIG.set("fs.file.impl.disable.cache", "true");
    // If the location URI doesn't contain any schema, fall back to local.
    SPOOLING_CONFIG.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
  }

  private static final float STOP_SPOOLING_FRACTION = (float) 0.5;
  public static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  public static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;

  private enum SpoolingState {
    SPOOLING, // added batches will be spooled
    PAUSE_SPOOLING, // added batches won't be spooled
    CLOSING // spooler is being closed, spooling is no longer necessary
  }

  /**
   * Each batch can have one of the following states:
   * <ul>
   *     <li>available in memory</li>
   *     <li>waiting to be spilled, but still available in memory</li>
   *     <li>spilled to disk</li>
   * </ul>
   */
  private enum BatchState {
    AVAILABLE,
    WAIT_TO_SPILL,
    SPILLED
  }

  private final BufferAllocator parentAllocator;
  private final long threshold;
  private final int oppositeId;
  private final int bufferIndex;

  // spoolingState and currentSizeInMemory can be accessed by both the fragment and fabric threads
  private AtomicReference<SpoolingState> spoolingState = new AtomicReference<>(SpoolingState.PAUSE_SPOOLING);
  private volatile long currentBatchesInMemory = 0;

  private BufferAllocator allocator = null;
  private SpillFile spillFile;
  private FSDataOutputStream outputStream;
  private FSDataInputStream inputStream;
  private long inputStreamLastKnownLen;
  private final FragmentWorkQueue workQueue;
  private final DeferredException deferred = new DeferredException();
  private SpillManager spillManager;
  private SpillService spillService;

  public SpoolingRawBatchBuffer(SharedResource resource, final SabotConfig config, FragmentWorkQueue workQueue,
                                FragmentHandle handle, SpillService spillService, BufferAllocator parentAllocator,
                                int fragmentCount, int oppositeId, int bufferIndex) {
    super(resource, config, handle, parentAllocator, fragmentCount);
    this.threshold = config.getLong(ExecConstants.SPOOLING_BUFFER_SIZE);
    this.oppositeId = oppositeId;
    this.bufferIndex = bufferIndex;
    this.bufferQueue = new SpoolingBufferQueue();
    this.workQueue = workQueue;
    this.spillService = spillService;
    this.inputStream = null;
    this.inputStreamLastKnownLen = 0;
    this.parentAllocator = parentAllocator;

    workQueue.put(new Runnable() {
      @Override
      public void run() {
        setupOutputStream();
      }
    });

  }

  @Override
  public void init() {
      final String name = String.format("%s:spoolingBatchBuffer", QueryIdHelper.getFragmentId(handle));
      this.allocator = parentAllocator.newChildAllocator(name, ALLOCATOR_INITIAL_RESERVATION, ALLOCATOR_MAX_RESERVATION);
  }

  private void setupOutputStream() {
    try {
      final String qid = QueryIdHelper.getQueryId(handle.getQueryId());
      final int majorFragmentId = handle.getMajorFragmentId();
      final int minorFragmentId = handle.getMinorFragmentId();
      final String id = String.format("spool-%s.%s.%s.%s.%s",
          qid, majorFragmentId, minorFragmentId, oppositeId, bufferIndex);

      this.spillManager = new SpillManager(config, null, id, SPOOLING_CONFIG, spillService, "spooling sorted exchange", null);
      this.spillFile = spillManager.getSpillFile("batches");
      outputStream = spillFile.create();
    } catch(Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  private class SpoolingBufferQueue implements BufferQueue<RawFragmentBatchWrapper> {

    private final LinkedBlockingDeque<RawFragmentBatchWrapper> buffer = Queues.newLinkedBlockingDeque();

    public SpoolingBufferQueue(){

    }

    @Override
    public RawFragmentBatch poll() {
      Exception ex = SpoolingRawBatchBuffer.this.deferred.getAndClear();
      if(ex != null){
        throw new RuntimeException(ex);
      }
      RawFragmentBatchWrapper batchWrapper = buffer.poll();
      if (batchWrapper != null) {
        return batchWrapper.get();
      }
      return null;
    }

    @Override
    public int size() {
      return buffer.size();
    }

    @Override
    public boolean isEmpty() {
      return buffer.size() == 0;
    }


    @Override
    public void add(RawFragmentBatchWrapper batchWrapper) {
      buffer.add(batchWrapper);
    }

    @Override
    public void clear() {
      RawFragmentBatchWrapper batchWrapper;
      RawFragmentBatch batch;
      while (!buffer.isEmpty()) {
        batchWrapper = buffer.poll();
        if (batchWrapper.isWaitingToSpill()) {
          logger.debug("unspilled buffer, sending ack");
          batchWrapper.batch.sendOk();
        }
        if(batchWrapper.state != BatchState.SPILLED) {
          batch = batchWrapper.get();
          if (batch.getBody() != null) {
            batch.getBody().close();
          }
        }
      }
    }
  }

  private boolean isCurrentlySpooling() {
    return spoolingState.get() == SpoolingState.SPOOLING;
  }

  private void startSpooling() {
    // don't start the spooler if it's being closed
    spoolingState.compareAndSet(SpoolingState.PAUSE_SPOOLING, SpoolingState.SPOOLING);
  }

  private void pauseSpooling() {
    // don't pause the spooler if it's being closed
    spoolingState.compareAndSet(SpoolingState.SPOOLING, SpoolingState.PAUSE_SPOOLING);
  }

  private boolean isClosing() {
    return spoolingState.get() == SpoolingState.CLOSING;
  }

  private void stopSpooling() {
    spoolingState.set(SpoolingState.CLOSING);
  }

  @Override
  protected void enqueueInner(RawFragmentBatch batch) {
    assert batch.getHeader().getSendingMajorFragmentId() == oppositeId;

    logger.debug("Enqueue batch. Current buffer size: {}. Sending fragment: {}", bufferQueue.size(), batch.getHeader().getSendingMajorFragmentId());
    RawFragmentBatchWrapper wrapper;

    boolean spoolCurrentBatch = isCurrentlySpooling();
    wrapper = new RawFragmentBatchWrapper(batch, !spoolCurrentBatch);
    currentBatchesInMemory++;
    if (spoolCurrentBatch) {
      addBatchForSpooling(wrapper);
    }
    bufferQueue.add(wrapper);
    if (!spoolCurrentBatch && currentBatchesInMemory >= threshold) {
      logger.debug("Buffer size {} greater than threshold {}. Start spooling to disk", currentBatchesInMemory, threshold);
      startSpooling();
    }
  }

  @Override
  protected void upkeep(RawFragmentBatch batch) {
    ArrowBuf body = batch.getBody();
    if (body != null) {
      currentBatchesInMemory--;
    }
    if (isCurrentlySpooling() && currentBatchesInMemory < threshold * STOP_SPOOLING_FRACTION) {
      logger.debug("buffer size {} less than {}x threshold. Stop spooling.", currentBatchesInMemory, STOP_SPOOLING_FRACTION);
      pauseSpooling();
    }
    logger.debug("Got batch. Current buffer size: {}", bufferQueue.size());
  }

  @Override
  public void close() throws Exception {

    stopSpooling();

    final AutoCloseable superCloser = new AutoCloseable(){
      @Override
      public void close() throws Exception {
        bufferQueue.clear();
        SpoolingRawBatchBuffer.super.close();
      }};

    AutoCloseables.close(allocator, outputStream, inputStream, spillFile, this.spillManager, superCloser, deferred);
  }


  private void addBatchForSpooling(final RawFragmentBatchWrapper batchWrapper) {
    if (!isClosing()) {
      workQueue.put(new Runnable(){
        @Override
        public void run() {
          try {
            if (isClosing() || !batchWrapper.isWaitingToSpill()) {
              return;
            }

            batchWrapper.writeToStream(outputStream);
          } catch (Throwable e) {
            deferred.addThrowable(e);
          }
        }});
    } else {
      // will not spill this batch
      batchWrapper.state = BatchState.AVAILABLE;
      batchWrapper.batch.sendOk();
    }
  }

  class RawFragmentBatchWrapper {
    private RawFragmentBatch batch;
    private BatchState state;
    private int bodyLength;
    private int totalLength;
    private long start = -1;
    private long check;

    public RawFragmentBatchWrapper(RawFragmentBatch batch, boolean available) {
      Preconditions.checkNotNull(batch);
      this.batch = batch;
      this.state = available ? BatchState.AVAILABLE : BatchState.WAIT_TO_SPILL;
      if (available) {
        batch.sendOk();
      }
    }

    boolean isWaitingToSpill() {
      return state == BatchState.WAIT_TO_SPILL;
    }

    public boolean isNull() {
      return batch == null;
    }

    public RawFragmentBatch get() {
      if (state != BatchState.SPILLED) {
        assert batch.getHeader() != null : "batch header null";
        state = BatchState.AVAILABLE;
        return batch;
      } else {
        try{
          readFromStream();
          return batch;
        }catch(Exception e){
          throw Throwables.propagate(e);
        }
      }
    }

    public long getBodySize() {
      if (batch.getBody() == null) {
        return 0;
      }
      assert batch.getBody().readableBytes() >= 0;
      return batch.getBody().getPossibleMemoryConsumed();
    }

    public void writeToStream(FSDataOutputStream stream) throws IOException {
      Stopwatch watch = Stopwatch.createStarted();
      ArrowBuf buf = null;
      try {
        check = ThreadLocalRandom.current().nextLong();
        start = stream.getPos();
        logger.debug("Writing check value {} at position {}", check, start);
        stream.writeLong(check);
        batch.getHeader().writeDelimitedTo(stream);
        buf = batch.getBody();
        if (buf != null) {
          bodyLength = LargeMemoryUtil.checkedCastToInt(buf.capacity());
        } else {
          bodyLength = 0;
        }
        if (bodyLength > 0) {
          buf.getBytes(0, stream, bodyLength);
        }
        stream.hsync();
        FileStatus status = spillFile.getFileStatus();
        long len = status.getLen();
        logger.debug("After spooling batch, stream at position {}. File length {}", stream.getPos(), len);
        assert start <= len : String.format("write pos %d is greater than len %d", start, len);
        totalLength = Math.toIntExact(len - start);
        long t = watch.elapsed(TimeUnit.MICROSECONDS);
        logger.debug("Took {} us to spool {} to disk. Rate {} mb/s", t, bodyLength, bodyLength / t);
      } finally {
        // even if the try block throws an exception we still want to send an ACK and release the lock
        // the caller will add the exception to deferred attribute and it will be thrown when the poll() method is called
        try {
          batch.sendOk(); // this can also throw an exception
        } finally {
          state = BatchState.SPILLED;
          batch = null;
          if (buf != null) {
            buf.close();
          }
        }
      }
    }

    public void readFromStream() throws IOException, InterruptedException {
      long pos = start;
      boolean tryAgain = true;
      int duration = 0;

      while (tryAgain) {

        // Sometimes, the file isn't quite done writing when we attempt to read it. As such, we need to wait and retry.
        Thread.sleep(duration);

        try (final ArrowBuf buf = allocator.buffer(bodyLength)) {
          if (start + totalLength > inputStreamLastKnownLen) {
            // Try to reuse the inputStream opened for earlier reads, but if current buffer was writtenToStream
            // after this inputStream was opened, close and reopen a new one to avoid EOF or short read errors.
            if (inputStream != null) {
              inputStream.close();
              inputStream = null;
            }
            long newLen = spillFile.getFileStatus().getLen();
            logger.debug("Opening new inputStream for file {}, start {}, totalLength {}, newLen {}",
              spillFile.getPath(), start, totalLength, newLen);
            assert newLen >= inputStreamLastKnownLen : String.format("newLen %d should not be less than prevLen %d",
              newLen, inputStreamLastKnownLen);
            assert newLen >= start + totalLength : String.format("file len %d too small for buffer, start %d, len %d",
                newLen, start, totalLength);
            inputStreamLastKnownLen = newLen;
            inputStream = spillFile.open();
          }
          inputStream.seek(start);
          final long currentPos = inputStream.getPos();
          final long check = inputStream.readLong();
          pos = inputStream.getPos();
          assert check == this.check : String.format("Check values don't match: %d %d, Position %d", this.check, check, currentPos);
          Stopwatch watch = Stopwatch.createStarted();
          FragmentRecordBatch header = FragmentRecordBatch.parseDelimitedFrom(inputStream);
          pos = inputStream.getPos();
          assert header != null : "header null after parsing from stream";
          // readIntoArrowBuf is a blocking operation. Safe to use COPY_BUFFER
          readIntoArrowBuf(inputStream, buf, bodyLength);
          pos = inputStream.getPos();
          batch = new RawFragmentBatch(header, buf, null);
          long t = watch.elapsed(TimeUnit.MICROSECONDS);
          logger.debug("Took {} us to read {} from disk. Rate {} mb/s", t, bodyLength, bodyLength / t);
          tryAgain = false;
          state = BatchState.AVAILABLE;
        } catch (EOFException e) {
          // Reset open inputStream
          if (inputStream != null) {
            inputStream.close();
            inputStream = null;
          }
          inputStreamLastKnownLen = 0;
          FileStatus status = spillFile.getFileStatus();
          logger.warn("EOF reading from file {} at pos {}. Current file size: {}. Read start {} & total length {}.",
            spillFile.getPath(), pos, status.getLen(), start, totalLength);
          duration = Math.max(1, duration * 2);
          if (duration < 60000) {
            continue;
          } else {
            throw e;
          }
        } finally {
          if (tryAgain) {
            // we had a premature exit, release batch memory so we don't leak it.
            if (batch != null) {
              batch.close();
            }
          }
        }
      }
    }
  }
}
