/*
 * Copyright (C) 2017 Dremio Corporation
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dremio.common.AutoCloseables;
import com.dremio.common.DeferredException;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentRecordBatch;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.store.LocalSyncableFileSystem;
import com.dremio.sabot.exec.fragment.FragmentWorkQueue;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Queues;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

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

  private final byte[] copyBuffer = new byte[64*1024];
  private final BufferAllocator allocator;
  private final long threshold;
  private final int oppositeId;
  private final int bufferIndex;

  // spoolingState and currentSizeInMemory can be accessed by both the fragment and fabric threads
  private AtomicReference<SpoolingState> spoolingState = new AtomicReference<>(SpoolingState.PAUSE_SPOOLING);
  private volatile long currentBatchesInMemory = 0;

  private SpillFile spillFile;
  private FSDataOutputStream outputStream;
  private final FragmentWorkQueue workQueue;
  private final DeferredException deferred = new DeferredException();
  private SpillManager spillManager;

  public SpoolingRawBatchBuffer(SharedResource resource, final SabotConfig config, FragmentWorkQueue workQueue, FragmentHandle handle, BufferAllocator allocator, int fragmentCount, int oppositeId, int bufferIndex) {
    super(resource, config, handle, allocator, fragmentCount);
    final String name = String.format("%s:spoolingBatchBuffer", QueryIdHelper.getFragmentId(handle));
    this.allocator = allocator.newChildAllocator(name, ALLOCATOR_INITIAL_RESERVATION, ALLOCATOR_MAX_RESERVATION);
    this.threshold = config.getLong(ExecConstants.SPOOLING_BUFFER_SIZE);
    this.oppositeId = oppositeId;
    this.bufferIndex = bufferIndex;
    this.bufferQueue = new SpoolingBufferQueue();
    this.workQueue = workQueue;

    workQueue.put(new Runnable() {
      @Override
      public void run() {
        setupOutputStream();
      }
    });

  }

  private void setupOutputStream() {
    try {
      final String qid = QueryIdHelper.getQueryId(handle.getQueryId());
      final int majorFragmentId = handle.getMajorFragmentId();
      final int minorFragmentId = handle.getMinorFragmentId();
      final String id = String.format("spool-%s.%s.%s.%s.%s",
          qid, majorFragmentId, minorFragmentId, oppositeId, bufferIndex);

      this.spillManager = new SpillManager(config, null, id, SPOOLING_CONFIG, "spooling sorted exchange");
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
        SpoolingRawBatchBuffer.super.close();
      }};

    AutoCloseables.close(allocator, outputStream, spillFile, this.spillManager, superCloser, deferred);
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
      ByteBuf buf = null;
      try {
        check = ThreadLocalRandom.current().nextLong();
        start = stream.getPos();
        logger.debug("Writing check value {} at position {}", check, start);
        stream.writeLong(check);
        batch.getHeader().writeDelimitedTo(stream);
        buf = batch.getBody();
        if (buf != null) {
          bodyLength = buf.capacity();
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
            buf.release();
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

        try(final FSDataInputStream stream = spillFile.open();
            final ArrowBuf buf = allocator.buffer(bodyLength)) {
          stream.seek(start);
          final long currentPos = stream.getPos();
          final long check = stream.readLong();
          pos = stream.getPos();
          assert check == this.check : String.format("Check values don't match: %d %d, Position %d", this.check, check, currentPos);
          Stopwatch watch = Stopwatch.createStarted();
          FragmentRecordBatch header = FragmentRecordBatch.parseDelimitedFrom(stream);
          pos = stream.getPos();
          assert header != null : "header null after parsing from stream";
          readIntoArrowBuf(stream, buf, bodyLength, copyBuffer);
          pos = stream.getPos();
          batch = new RawFragmentBatch(header, buf, null);
          long t = watch.elapsed(TimeUnit.MICROSECONDS);
          logger.debug("Took {} us to read {} from disk. Rate {} mb/s", t, bodyLength, bodyLength / t);
          tryAgain = false;
          state = BatchState.AVAILABLE;
        } catch (EOFException e) {
          FileStatus status = spillFile.getFileStatus();
          logger.warn("EOF reading from file {} at pos {}. Current file size: {}", spillFile.getPath(), pos, status.getLen());
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
              batch.getBody().release();
            }
          }
        }
      }
    }
  }
}
