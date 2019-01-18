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
package com.dremio.sabot.op.sort.external;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.cache.VectorAccessibleSerializable;
import com.dremio.exec.compile.sig.GeneratorMapping;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.WritableBatch;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.exec.store.LocalSyncableFileSystem;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.vector.CopyUtil;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.copier.Copier;
import com.dremio.sabot.op.copier.CopierOperator;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.dremio.service.spill.SpillService;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Maintains 0..N separate runs of sorted data on disk, each in its own file.
 *
 * Also exposes an ability to live merge and copy the streams back.
 */
public class DiskRunManager implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DiskRunManager.class);

  private final List<Ordering> orderings;
  private final List<DiskRun> diskRuns = new CopyOnWriteArrayList<>();
  private final ClassProducer producer;
  private final BatchSchema dataSchema;

  private final Stopwatch spillWatch = Stopwatch.createUnstarted();
  private final Stopwatch mergeWatch = Stopwatch.createUnstarted();

  private int run = 0;
  private int merge = 0;
  private final BufferAllocator parentAllocator;
  private BufferAllocator copierAllocator;
  private final int targetRecordCount; /** number of records for the copy output. */
  private final int targetBatchSizeInBytes; /** estimated size of copy output */
  private DiskRunMerger diskRunMerger;
  private PriorityQueueCopier copier;
  private VectorContainer tempContainer;
  private MergeState mergeState = MergeState.TRY;
  private final SpillManager spillManager;
  private boolean compressSpilledBatch;
  private BufferAllocator compressSpilledBatchAllocator;
  private final ExternalSortTracer tracer;
  private long totalDataSpilled;

  private enum MergeState {
    TRY, // Try to reserve memory to copy all runs
    MERGE, // We were unable to reserve memory for copy, so will attempt to merge some runs
    COPY // We succeeded in reserving memory for copy, so now merge and copy runs to output
  }

  public DiskRunManager(
      SabotConfig config,
      OptionManager optionManager,
      int targetRecordCount,
      int targetBatchSizeInBytes,
      FragmentHandle handle,
      int operatorId,
      ClassProducer producer,
      BufferAllocator parentAllocator,
      List<Ordering> orderings,
      BatchSchema dataSchema,
      boolean compressSpilledBatch,
      ExternalSortTracer tracer,
      SpillService spillService
      ) throws Exception {
    try (RollbackCloseable rollback = new RollbackCloseable()) {
      this.targetRecordCount = targetRecordCount;
      this.targetBatchSizeInBytes = targetBatchSizeInBytes;
      this.orderings = orderings;
      this.producer = producer;
      this.dataSchema = dataSchema;
      this.parentAllocator = parentAllocator;
      this.compressSpilledBatch = compressSpilledBatch;
      this.tracer = tracer;
      this.totalDataSpilled = 0;
      if (compressSpilledBatch) {
        long reserve = VectorAccessibleSerializable.RAW_CHUNK_SIZE_TO_COMPRESS * 2;
        compressSpilledBatchAllocator = this.parentAllocator.newChildAllocator("spill_with_snappy", reserve, Long.MAX_VALUE);
        rollback.add(compressSpilledBatchAllocator);
      }

      final Configuration conf = FileSystemPlugin.getNewFsConf();
      conf.set(SpillManager.DREMIO_LOCAL_IMPL_STRING, LocalSyncableFileSystem.class.getName());
      // If the location URI doesn't contain any schema, fall back to local.
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);

      final String id = String.format("esort-%s.%s.%s.%s", QueryIdHelper.getQueryId(handle.getQueryId()),
          handle.getMajorFragmentId(), handle.getMinorFragmentId(), operatorId
      );
      this.spillManager = new SpillManager(config, optionManager, id, conf, spillService, "sort spilling");
      rollback.add(this.spillManager);

      rollback.commit();
    }
  }

  public long spillTimeNanos() {
    return spillWatch.elapsed(TimeUnit.NANOSECONDS);
  }

  public long mergeTimeNanos() {
    return mergeWatch.elapsed(TimeUnit.NANOSECONDS);
  }

  public int spillCount() {
    return run;
  }

  public int mergeCount() {
    return merge;
  }

  /**
   * The first time this is called, it will try to reserve enough memory to handle merging and copying of all disk runs.
   * If it fails to reserve, it will create a DiskRunMerger that will be used to merge some subset of the runs into a single run
   * Subsequent calls to this method will merge, copy and spill one batch of data.
   *
   * After all of the disk runs in the DiskRunMerger have been merged, the newly created diskrun is added to the list,
   * and the process repeats
   * @return true if we successfully reserved enough memory to handle all disk runs
   */
  public boolean consolidateAsNecessary() {
    Preconditions.checkState(mergeState != MergeState.COPY, "Can't consolidate after copy has begun");
    if (mergeState == MergeState.MERGE) {
      try {
        if (diskRunMerger.consolidate()) {
          diskRunMerger.close();
          diskRunMerger = null;
          mergeState = MergeState.TRY;
        } else {
          return false;
        }
      } catch (IOException e) {
        throw UserException.dataReadError(e).message("Failure while attempting to merge spilled data")
          .build(logger);
      }
    }
    try {
      getCopierAllocator(diskRuns);
      mergeState = MergeState.COPY;
      return true;
    } catch (OutOfMemoryException e) {
      mergeState = MergeState.MERGE;
      // reattempt with smaller list
    }

    // We failed to reserve memory to handle all runs, so attempt to merge some runs

    int runsToMerge = diskRuns.size() / 2;
    List<DiskRun> runList = null;
    while (true) {
      try {
        runList = ImmutableList.copyOf(diskRuns.subList(0, runsToMerge));
        getCopierAllocator(runList);
        try (RollbackCloseable rollback = new RollbackCloseable()) {
          diskRunMerger = rollback.add(new DiskRunMerger(runList));

          diskRunMerger.init();

          rollback.commit();
        }
        return false;
      } catch (OutOfMemoryException e) {
        // reattempt with smaller list
        runsToMerge /= 2;
        diskRunMerger = null;
        for (DiskRun run : runList) {
          run.resetOpenStatus();
        }

        if (runsToMerge < 2) {
          final String message = "DiskRunManager: Unable to secure enough memory to merge spilled sort data.";
          final long totalMaxBatchSizeAllRuns = getMaxBatchSizeAllRuns(runList);
          final long reservation = totalMaxBatchSizeAllRuns + (targetBatchSizeInBytes * 3);
          /* we are here for OOM because we couldnt't create a copy allocator for loading batches from even 2 disk runs
           * so record all the information including how much copy allocator tried to reserve before it failed.
           * see getCopierAllocator, the computation has been borrowed from that function.
           */
          tracer.reserveMemoryForDiskRunCopyOOMEvent(reservation, Long.MAX_VALUE, totalMaxBatchSizeAllRuns);
          tracer.setDiskRunState(diskRuns.size(), spillCount(), mergeCount(), getMaxBatchSize());
          tracer.setDiskRunCopyAllocatorState(copierAllocator);
          tracer.setExternalSortAllocatorState(parentAllocator);
          throw tracer.prepareAndThrowException(e, message);
        }
      } catch (Exception e) {
        throw UserException.dataReadError(e).message("Failure while attempting to merge spilled data")
            .build(logger);
      }
    }
  }

  private long getMaxBatchSizeAllRuns(List<DiskRun> diskRuns) {
    long totalMax = 0;
    for(DiskRun run : diskRuns){
      long batchSize = nextPowerOfTwo(run.largestBatch);
      totalMax += batchSize;
    }
    return totalMax;
  }

  private void removeDiskRuns(int toRemove) throws Exception {
    for (int i = 0; i < toRemove; i++) {
      DiskRun run = diskRuns.remove(0);
      if (run != null) {
        run.close();
      }
    }
  }

  /**
   * Return the total amount of data (in bytes) spilled by {@link ExternalSortOperator}
   * @return total size (in bytes) of data spilled
   */
  long getTotalDataSpilled() {
    return totalDataSpilled;
  }

  public int getAvgMaxBatchSize() {
    if (diskRuns.size() == 0) {
      return 0;
    }
    int totalSize = 0;
    for (DiskRun run : diskRuns) {
      totalSize += run.largestBatch;
    }
    return totalSize / diskRuns.size();
  }

  public int getMaxBatchSize() {
    int maxSize = 0;
    for (DiskRun run : diskRuns) {
      maxSize = Math.max(maxSize, run.largestBatch);
    }
    return maxSize;
  }

  public int getMedianMaxBatchSize() {
    if (diskRuns.size() == 0) {
      return 0;
    }
    return FluentIterable.from(diskRuns)
      .transform(new Function<DiskRun, Integer>() {
        @Override
        public Integer apply(DiskRun diskRun) {
          return diskRun.largestBatch;
        }
      })
      .toSortedList(new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return Integer.compare(o1, o2);
        }
      })
      .get(diskRuns.size() / 2);
  }

  private class DiskRunMerger implements AutoCloseable {
    private final List<DiskRun> diskRuns;

    private PriorityQueueCopier copier;
    private FSDataOutputStream out;
    private VectorContainer container;
    private SpillFile spillFile;

    private int maxBatchSize = 0;
    private int recordCount;
    private int batchCount = 0;

    public DiskRunMerger(List<DiskRun> diskRuns) {
      this.diskRuns = diskRuns;
      this.spillFile = spillManager.getSpillFile(String.format("merge%05d", merge++));
    }

    public void init() throws Exception {
      try (RollbackCloseable rollback = new RollbackCloseable()) {
        container = rollback.add(VectorContainer.create(copierAllocator, dataSchema));
        copier = rollback.add(createCopier(container, diskRuns));
        out = rollback.add(spillFile.create());
        rollback.commit();
      }
    }

    public boolean consolidate() throws IOException {
      mergeWatch.start();
      try {
        int copied = copier.copy(targetRecordCount);
        if (copied == 0) {
          out.close();
          DiskRun diskRun = new DiskRun(spillFile, recordCount, maxBatchSize, batchCount);
          DiskRunManager.this.diskRuns.add(diskRun);
          return true;
        }
        recordCount += copied;
        int batchSize = spillBatch(container, copied, out);
        container.zeroVectors();
        maxBatchSize = Math.max(maxBatchSize, batchSize);
        batchCount++;
        return false;
      } finally {
        mergeWatch.stop();
      }
    }

    @Override
    public void close() {
      try {
        AutoCloseables.close(copier, out, container);
        removeDiskRuns(this.diskRuns.size());
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }

  public void spill(VectorContainer hyperBatch, BufferAllocator copyTargetAllocator) throws Exception {
    /* as per MemoryRun, for copyTargetAllocator, initReservation and maxAllocation are same */
    logger.debug("DiskRunManager-Spill: spill copy allocator reservation {} spill copy allocator limit {}", copyTargetAllocator.getInitReservation(), copyTargetAllocator.getLimit());
    spillWatch.start();
    try {
      int maxBatchSize = 0;
      int batchCount = 0;
      int records = 0;
      int recordCount = 0;
      int recordsSpilledInCurrentIteration = 0;
      int remainingRecordCount = 0;
      final SpillFile spillFile = spillManager.getSpillFile(String.format("run%05d", run++));
      BatchSchema outgoingSchema = null;

      try (FSDataOutputStream out = spillFile.create();
           final VectorContainer outgoing = VectorContainer.create(copyTargetAllocator, hyperBatch.getSchema());
           VectorContainer hyperBatchToClose = hyperBatch) {

        final Copier copier = CopierOperator.getGenerated4Copier(
          producer,
          hyperBatch,
          outgoing);
        final SelectionVector4 sv4 = hyperBatch.getSelectionVector4();
        outgoingSchema = outgoing.getSchema().clone();

        do {
          recordCount = sv4.getCount();
          if (recordCount == 0) {
            continue;
          }
          /* Set initial capacity so that each vector will allocate just what it needs,
           * and the total allocation will fit in the reserved amount.
           *
           * setInitialCapacity will change the state of variables used to allocate
           * memory for vectors during the call to allocateNew() later in copyRecords().
           * Since the actual allocation happens inside the while loop by copyRecords(),
           * for each call to copyRecords(), we don't need to allocate memory for
           * number of records we started with so its better to invoke setInitialCapacity
           * again to ensure reduced allocation. For example:
           *
           * 1. say MemoryRun gave us a batch of size 4096 to spill, i.e sv4.getCount() is 4096
           * 2. we did setInitialCapacity(4096) on the vectors inside the outgoing container that
           * will be spilled.
           * 3. we then entered the while loop to start copy records from the incoming batch given to
           * us by MemoryRun.closeToDisk().
           * 4. Say in first iteration of copyRecords(), we copied only 2048 records.
           * 5. We spilled the currently copied batch and move onto copying next 2048 records after
           * clearing the outgoing container and releasing memory with underlying buffers.
           * 6. In the next call to copyRecords, we again allocate memory for vectors in outgoing.
           * But we allocated memory for 4096 records because that was the state we recorded when
           * doing the setInitialCapacity() in first place. This is the reason why it is
           * probably better to do setInitialCapacity() before each call to copyRecords.
           */
          logger.debug("DiskRunManager: Copy {} records", recordCount);
          remainingRecordCount = recordCount;
          recordsSpilledInCurrentIteration = 0;
          while (recordsSpilledInCurrentIteration < recordCount) {
            final int copied = copier.copyRecords(recordsSpilledInCurrentIteration, remainingRecordCount);
            assert copied > 0 : "couldn't copy any rows, probably run out of memory while doing so";
            outgoing.setAllCount(copied);
            int batchSize = spillBatch(outgoing, copied, out);
            totalDataSpilled += batchSize;
            recordsSpilledInCurrentIteration += copied;
            maxBatchSize = Math.max(maxBatchSize, batchSize);
            batchCount++;
            logger.debug("spilled a batch of records {}", copied);
            outgoing.zeroVectors();
            remainingRecordCount = recordCount - recordsSpilledInCurrentIteration;
          }

          records += recordCount;
        } while (sv4.next());

      } catch (OutOfMemoryException ex) {
        /*
         * this is thrown by Copier if it fails to copy a single record.
         * if the copier catches OOM after copying one or more records, then
         * we proceed with spilling whatever we copied and move onto next
         * iteration of <allocate, copy, spill>.
         */
        logger.debug("DiskRunManager: Out of Memory while trying to copy and spill data");
        tracer.setBatchesSpilled(batchCount);
        tracer.setTotalRecordsSpilled(records);
        tracer.setRecordsToSpillInCurrentIteration(recordCount);
        tracer.setRecordsSpilledInCurrentIteration(recordsSpilledInCurrentIteration);
        tracer.setSchemaOfBatchToSpill(outgoingSchema.toString());
        tracer.setInitialCapacityForCurrentSpillIteration(remainingRecordCount);
        tracer.setMaxBatchSizeSpilled(maxBatchSize);
        tracer.setSpillCopyAllocatorState(copyTargetAllocator);
        tracer.setDiskRunState(diskRuns.size(), spillCount(), mergeCount(), getMaxBatchSize());
        tracer.setDiskRunCopyAllocatorState(copierAllocator);
        tracer.setExternalSortAllocatorState(parentAllocator);

        final String message = "DiskRunManager: Failure while spilling sort data to disk";
        throw tracer.prepareAndThrowException(ex, message);
      }

      Preconditions.checkArgument(copyTargetAllocator.getAllocatedMemory() == 0,
        "Target Allocator should be empty, is consuming %s bytes.", copyTargetAllocator.getAllocatedMemory());
      final DiskRun run = new DiskRun(spillFile, records, maxBatchSize, batchCount);
      diskRuns.add(run);
    } finally {
      spillWatch.stop();
    }
  }

  private int spillBatch(VectorContainer outgoing, int records, OutputStream out) throws IOException {
    try (WritableBatch batch = WritableBatch.getBatchNoHVWrap(records, outgoing, false)) {
      int batchSize = batch.getLength();
      VectorAccessibleSerializable outputBatch;

      if (compressSpilledBatch) {
        /* compression enabled - compress the spill batch.
         * a valid allocator that will be used to allocate the memory for compressed buffers.
         * this allocator will _only_ be used to allocate buffer used to store and write the compressed
         * data.
         */
        outputBatch = new VectorAccessibleSerializable(batch, null, compressSpilledBatchAllocator, true);
      }
      else {
        /* no need for an allocator on the spill path if compression is not enabled */
        outputBatch = new VectorAccessibleSerializable(batch, null);
      }

      // write length and data to file.
      Stopwatch watch = Stopwatch.createStarted();
      outputBatch.writeToStream(out);

      logger.debug("Took {} us to spill {} records", watch.elapsed(TimeUnit.MICROSECONDS), records);
      return batchSize;
    }
  }

  public boolean isEmpty() {
    return diskRuns.isEmpty();
  }


  private DiskRunIterator[] getIterators(BufferAllocator allocator, List<DiskRun> diskRuns, ExpandableHyperContainer container) throws Exception {
    final DiskRunIterator[] iterators = new DiskRunIterator[diskRuns.size()];
    try (RollbackCloseable rollback = new RollbackCloseable()) {
      for (int i = 0; i < diskRuns.size(); i++) {
        final DiskRun run = diskRuns.get(i);
        iterators[i] = run.openRun(allocator, i, container);
        rollback.add(iterators[i]);
      }
      rollback.commit();
    }
    return iterators;
  }

  private void getCopierAllocator(List<DiskRun> diskRuns) {
    if (copierAllocator != null) {
      copierAllocator.close();
      copierAllocator = null;
    }

    long totalSizeNeeded = 0;
    // for now we always read one batch from all disk runs, so we need to make sure we have enough memory reserved
    // to allocate the largest batch per run
    for(DiskRun run : diskRuns){
      long batchSize = nextPowerOfTwo(run.largestBatch);
      totalSizeNeeded += batchSize;
    }

    // add the required space for the copy output. We use * 3 to manage against a really large vector.
    totalSizeNeeded += targetBatchSizeInBytes * 3;

    // because we can't know for sure how much memory will be needed for variable length vectors we don't put a limit
    // on the copy allocator. But this will still be capped by the sort allocator limit.
    copierAllocator = this.parentAllocator.newChildAllocator("spill_copier", totalSizeNeeded, Long.MAX_VALUE);
  }

  public PriorityQueueCopier createCopier() throws Exception {
    Preconditions.checkState(tempContainer == null);
    Preconditions.checkState(mergeState == MergeState.COPY);
    if (copier != null) {
      AutoCloseables.closeNoChecked(copier);
    }
    tempContainer = VectorContainer.create(copierAllocator, dataSchema);
    return createCopier(tempContainer, this.diskRuns);
  }

  public void transferOut(VectorContainer output, int recordCount) {
    Preconditions.checkState(mergeState == MergeState.COPY);
    tempContainer.setAllCount(recordCount);
    tempContainer.transferOut(output);
  }

  private PriorityQueueCopier createCopier(VectorContainer targetContainer, List<DiskRun> diskRuns) throws Exception {
    if (this.copier != null) {
      try {
        this.copier.close();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }

    final GeneratorMapping copierMapping = new GeneratorMapping("doSetup", "doCopy", null, null);
    final MappingSet mainMappingSet = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet copierMappingSet = new MappingSet(copierMapping, copierMapping);

    final ExpandableHyperContainer incoming = new ExpandableHyperContainer(copierAllocator, dataSchema);
    try (RollbackCloseable rollback = new RollbackCloseable()) {
      final DiskRunIterator[] iterators = getIterators(copierAllocator, diskRuns, incoming);
      rollback.addAll(iterators);

      final CodeGenerator<PriorityQueueCopier> cg = producer.createGenerator(PriorityQueueCopier.TEMPLATE_DEFINITION);

      ClassGenerator<PriorityQueueCopier> g = cg.getRoot();
      ExternalSortOperator.generateComparisons(g, incoming, orderings, producer);
      g.setMappingSet(copierMappingSet);
      CopyUtil.generateCopies(g, incoming, true);
      g.setMappingSet(mainMappingSet);

      final PriorityQueueCopier copier = cg.getImplementationClass();

      copier.setup(producer.getFunctionContext(), copierAllocator, iterators, incoming, targetContainer);
      this.copier = copier;

      rollback.commit();

      return copier;
    }
  }


  @Override
  public void close() throws Exception {
    AutoCloseables.close(Iterables.concat(this.diskRuns, Collections.singleton(diskRunMerger),
      Collections.singleton(compressSpilledBatchAllocator), Collections.singleton(this.spillManager), Collections.singleton(copierAllocator)));
  }

  private class DiskRun implements AutoCloseable {

    private final SpillFile spillFile;
    private final int recordCount;
    private final int largestBatch;
    private final int batchCount;
    private DiskRunIterator iterator;

    public DiskRun(SpillFile spillFile, int recordCount, int largestBatch, int batchCount) {
      super();
      this.spillFile = spillFile;
      this.recordCount = recordCount;
      this.largestBatch = largestBatch;
      this.batchCount = batchCount;
    }

    public void resetOpenStatus() {
      iterator = null;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(spillFile);
    }

    /**
     * Opens an iterator to read read batches from spill run. It is responsibility of the caller to release iterator
     * when done.
     *
     * @param batchId
     * @param container
     * @return
     * @throws IOException
     */
    private DiskRunIterator openRun(BufferAllocator copierAllocator, int batchId, ExpandableHyperContainer container) throws IOException{
      Preconditions.checkState(iterator == null);
      final long memCapacity = nextPowerOfTwo(largestBatch);
      final BufferAllocator allocator = copierAllocator.newChildAllocator("diskrun", 0, memCapacity);
      iterator = new DiskRunIterator(batchCount, spillFile, container, allocator);

      return iterator;
    }

  }

  public static int nextPowerOfTwo(int val) {
    int highestBit = Integer.highestOneBit(val);
    if (highestBit == val) {
      return val;
    } else {
      return highestBit << 1;
    }
  }

  public class DiskRunIterator implements AutoCloseable {
    private final BufferAllocator allocator;
    private FSDataInputStream inputStream;

    private int batchIndex = -1;
    private final int batchIndexMax;

    private int recordIndex = -1;
    private int recordIndexMax;
    private final VectorContainer container = new VectorContainer();

    /*
     * DiskRunIterator opens a spill file and loads batch(es) into memory when reading spill files.
     * As part of creation of iterator below, we load a single batch and if this IO fails, the
     * already opened spill file read stream will never be closed since the instantiation of
     * DiskRunIterator never succeeded. Using RollbackCloseable in the caller will also not
     * help for the same reason that failure happened during instantiation.
     */
    private DiskRunIterator(int batchCount, SpillFile spillFile, ExpandableHyperContainer hyperContainer, BufferAllocator allocator) throws IOException {
      try {
        this.allocator = allocator;
        this.inputStream = spillFile.open();
        this.batchIndexMax = batchCount;
        loadNextBatch(true);
        hyperContainer.addBatch(this.container);
      } catch (Exception e) {
        /* close spill file read stream if failure happened after stream was successfully opened */
        if (inputStream != null) {
          inputStream.close();
        }
        throw e;
      }
    }

    private void loadNextBatch(boolean first) throws IOException{
      Preconditions.checkArgument(batchIndex + 1 < batchIndexMax, "You tried to go beyond end of available batches to read.");
      container.zeroVectors();

      /* uncompress the data when de-serializing the spilled data into ArrowBufs */
      final VectorAccessibleSerializable serializer = new VectorAccessibleSerializable(allocator, compressSpilledBatch, compressSpilledBatchAllocator);
      serializer.readFromStream(inputStream);

      final VectorContainer incoming = serializer.get();
      Iterator<VectorWrapper<?>> wrapperIterator = incoming.iterator();

      if(first){
        for(VectorWrapper<?> wrapper : incoming){
          final ValueVector sourceVector = wrapper.getValueVector();
          TransferPair pair = sourceVector.getTransferPair(allocator);
          final ValueVector targetVector = pair.getTo();
          pair.transfer();
          container.add(targetVector);
        }
        container.buildSchema();
      }else{
        for (VectorWrapper<?> w : container) {
          final ValueVector sourceVector = wrapperIterator.next().getValueVector();
          final TransferPair pair = sourceVector.makeTransferPair(w.getValueVector());
          pair.transfer();
        }
      }

      recordIndexMax = incoming.getRecordCount();
      batchIndex++;
      recordIndex = -1;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(container, allocator, inputStream);
    }

    public int getNextId() throws IOException{
      while(recordIndex + 1 >= recordIndexMax){
        // no more records. try to load batch.
        if(batchIndex + 1 < batchIndexMax){
          loadNextBatch(false);
        }else{
          return -1;
        }
      }

      return ++recordIndex;
    }

  }
}
