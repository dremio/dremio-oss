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
package com.dremio.sabot.op.join.vhash.spill.partition;

import static com.dremio.sabot.op.common.ht2.LBlockHashTable.VAR_LENGTH_SIZE;
import static com.dremio.sabot.op.common.ht2.LBlockHashTable.VAR_OFFSET_SIZE;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.MemoryReleaser;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.dremio.sabot.op.join.vhash.spill.io.SpillChunk;
import com.dremio.sabot.op.join.vhash.spill.io.SpillFileDescriptor;
import com.dremio.sabot.op.join.vhash.spill.io.SpillSerializable;
import com.dremio.sabot.op.join.vhash.spill.list.ArrayOfEntriesView;
import com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.slicer.PageBatchSlicer;
import com.dremio.sabot.op.join.vhash.spill.slicer.RecordBatchPage;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Memory releaser for the in-memory hash-table, linked list & carry-overs.
 *
 * <p>For each batch, - write the build batch to disk (pivoted & unpivoted) - release the memory
 * used by the carry-over vectors for the batch After all batches are spilled, - release memory in
 * hash-table & linked list.
 */
public class BuildMemoryReleaser implements MemoryReleaser {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BuildMemoryReleaser.class);
  private static final int MAX_ENTRIES_PER_CHUNK = 4096;
  private final SpillFileDescriptor spillFile;
  private final PageListMultimap pageListMultimap;
  private final JoinTable joinTable;
  private final List<VectorContainer> listFromHyperContainer;
  private final List<RecordBatchPage> slicedBatchPages;
  private final List<AutoCloseable> closeables;
  private final ArrayOfEntriesView allEntries;
  private final int pivotSize;
  private final BufferAllocator allocatorToRelease; // do not allocate from this allocator
  private final PagePool spillPagePool;
  private final SpillSerializable serializable;
  private SpillManager.SpillOutputStream outputStream;
  private int cursor;
  private long recordsWritten;
  private long batchesWritten;

  BuildMemoryReleaser(
      JoinSetupParams setupParams,
      SpillFileDescriptor spillFile,
      PageListMultimap pageListMultimap,
      JoinTable joinTable,
      ExpandableHyperContainer hyperContainer,
      List<RecordBatchPage> slicedBatchPages,
      BufferAllocator allocatorToRelease,
      List<AutoCloseable> closeables) {
    this.spillFile = spillFile;
    this.pageListMultimap = pageListMultimap;
    this.joinTable = joinTable;
    this.listFromHyperContainer = hyperContainer.convertToComponentContainers();
    this.slicedBatchPages = slicedBatchPages;
    this.allocatorToRelease = allocatorToRelease;
    this.closeables = closeables;
    this.pivotSize = setupParams.getBuildKeyPivot().getBlockWidth();

    this.allEntries = pageListMultimap.findAll();
    this.cursor = allEntries.getFirstValidIndex();
    this.spillPagePool = setupParams.getSpillPagePool();
    this.serializable = setupParams.getSpillSerializable(true /*isBuild*/);
  }

  @Override
  public int run() throws Exception {
    recordsWritten += writeOneChunk();
    return 0;
  }

  @Override
  public boolean isFinished() {
    return cursor >= allEntries.size();
  }

  private int writeOneChunk() throws Exception {
    if (outputStream == null) {
      outputStream = spillFile.getFile().create(true);
    }
    if (isFinished()) {
      return 0;
    }

    int curBatchId = PageListMultimap.getBatchIdFromLong(allEntries.getCarryAlongId(cursor));
    int recordsDone = 0;
    try (Page pivotedPage = spillPagePool.newPage();
        Page unpivotedPage = spillPagePool.newPage();
        MiscBuffers buffers = new MiscBuffers(spillPagePool.newPage())) {
      int pickedRecords =
          pickMaxPivotedRecordsInBatch(curBatchId, pivotedPage.getPageSize(), buffers);
      Preconditions.checkState(pickedRecords > 0);
      PageBatchSlicer slicer =
          new PageBatchSlicer(
              spillPagePool, buffers.getSv2(), listFromHyperContainer.get(curBatchId));

      // copy unpivoted records that will fit into a page.
      try (RecordBatchPage batchData =
          slicer.copyToPageTillFull(unpivotedPage, 0, pickedRecords - 1)) {
        pickedRecords =
            batchData.getRecordCount(); // the slicer can pick lesser records than asked.
        Preconditions.checkState(pickedRecords > 0);

        ArrowBuf[] dstBufs = copyPivoted(pivotedPage, pickedRecords, buffers);
        try (SpillChunk chunk =
            new SpillChunk(
                pickedRecords,
                dstBufs[0],
                dstBufs[1],
                batchData.getContainer(),
                ImmutableList.of())) {
          serializable.writeChunkToStream(chunk, outputStream);
          recordsDone += batchData.getRecordCount();
          ++batchesWritten;
        }
      }
      cursor += pickedRecords;
    }
    int nextBatchId =
        cursor < allEntries.size() - 1
            ? PageListMultimap.getBatchIdFromLong(allEntries.getCarryAlongId(cursor - 1))
            : -1;
    if (curBatchId != nextBatchId) {
      // if the batch is fully processed, release the corresponding memory.
      releaseMemoryForBatch(curBatchId);
    }
    return recordsDone;
  }

  private int pickMaxPivotedRecordsInBatch(
      int targetBatchId, int maxCumulativeSize, MiscBuffers buffers) {
    final ArrowBuf keyOrdinalsBuf = buffers.getKeyOrdinalsBuf();
    final ArrowBuf outKeyLengthsBuf = buffers.getOutKeyLengthsBuf();
    final ArrowBuf sv2 = buffers.getSv2();
    int localCursor = cursor;

    // walk through the linked list & get the hash-table ordinals (only for current batchId).
    int numKeysInIteration = 0;
    while (numKeysInIteration < MAX_ENTRIES_PER_CHUNK) {
      if (localCursor >= allEntries.size()) {
        // reached end-of-list
        break;
      }

      final int tableOrdinal = allEntries.getKey(localCursor);
      final long carryAlongId = allEntries.getCarryAlongId(localCursor);
      int batchId = PageListMultimap.getBatchIdFromLong(carryAlongId);
      if (batchId != targetBatchId) {
        // do not step into a different batch, becomes hard to track which memory can be released
        break;
      }

      SV2UnsignedUtil.writeAtOffset(
          sv2,
          (numKeysInIteration) * SelectionVector2.RECORD_SIZE,
          PageListMultimap.getRecordIndexFromLong(carryAlongId));
      keyOrdinalsBuf.setInt(numKeysInIteration * VAR_OFFSET_SIZE, tableOrdinal);
      ++numKeysInIteration;
      ++localCursor;
    }

    // fetch the var lengths for the selected ordinals from the hash-table
    int numEntriesPicked = 0;
    int cumulativeSizeOfPickedRecords = 0;
    joinTable.getVarKeyLengths(keyOrdinalsBuf, numKeysInIteration, outKeyLengthsBuf);

    // pick as many records as will fit in the requested threshold (maxCumulativeSize)
    for (int i = 0; i < numKeysInIteration; ++i) {
      int varEntrySz = outKeyLengthsBuf.getInt(i * VAR_LENGTH_SIZE);
      if (cumulativeSizeOfPickedRecords + pivotSize + varEntrySz <= maxCumulativeSize) {
        cumulativeSizeOfPickedRecords += (pivotSize + varEntrySz);
        ++numEntriesPicked;
      } else {
        // this page is full.
        break;
      }
    }

    return numEntriesPicked;
  }

  private ArrowBuf[] copyPivoted(Page dstPage, int numRecords, MiscBuffers buffers) {
    final ArrowBuf keyOrdinalsBuf = buffers.getKeyOrdinalsBuf();
    for (int i = 0; i < numRecords; ++i) {
      final int tableOrdinal = allEntries.getKey(cursor + i);
      keyOrdinalsBuf.setInt(i * VAR_OFFSET_SIZE, tableOrdinal);
    }
    int cumulativeVarSize = joinTable.getCumulativeVarKeyLength(keyOrdinalsBuf, numRecords);
    ArrowBuf dstFixedBuf = dstPage.slice(numRecords * pivotSize);
    ArrowBuf dstVarBuf = dstPage.slice(cumulativeVarSize);

    joinTable.copyKeysToBuffer(keyOrdinalsBuf, numRecords, dstFixedBuf, dstVarBuf);
    return new ArrowBuf[] {dstFixedBuf, dstVarBuf};
  }

  private void releaseMemoryForBatch(int batchIdx) {
    VectorContainer container = listFromHyperContainer.get(batchIdx);
    listFromHyperContainer.set(batchIdx, null);
    container.close();

    RecordBatchPage page = slicedBatchPages.get(batchIdx);
    slicedBatchPages.set(batchIdx, null);
    page.close();
  }

  @Override
  public long getCurrentMemorySize() {
    return allocatorToRelease.getAllocatedMemory();
  }

  @Override
  public void close() throws Exception {
    if (outputStream != null) {
      spillFile.update(recordsWritten, outputStream.getWriteBytes());
      logger.debug(
          "spilled to {} records {} batches {} size {}",
          spillFile.getFile().getPath().getName(),
          recordsWritten,
          batchesWritten,
          outputStream.getWriteBytes());
    }
    AutoCloseables.close(
        listFromHyperContainer,
        slicedBatchPages,
        Lists.newArrayList(joinTable, pageListMultimap, outputStream),
        closeables,
        Lists.newArrayList(allocatorToRelease));
  }

  private static class MiscBuffers implements AutoCloseable {
    private final Page page;
    // buffer for key ordinals
    private final ArrowBuf keyOrdinalsBuf;
    // buffer to store key lengths
    private final ArrowBuf outKeyLengthsBuf;
    // buffer for sv2
    private final ArrowBuf sv2;

    MiscBuffers(Page page) {
      this.page = page;
      this.keyOrdinalsBuf = page.slice(MAX_ENTRIES_PER_CHUNK * VAR_OFFSET_SIZE);
      this.outKeyLengthsBuf = page.slice(MAX_ENTRIES_PER_CHUNK * VAR_LENGTH_SIZE);
      this.sv2 = page.slice(MAX_ENTRIES_PER_CHUNK * SelectionVector2.RECORD_SIZE);
    }

    ArrowBuf getKeyOrdinalsBuf() {
      return keyOrdinalsBuf;
    }

    ArrowBuf getOutKeyLengthsBuf() {
      return outKeyLengthsBuf;
    }

    ArrowBuf getSv2() {
      return sv2;
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(keyOrdinalsBuf, outKeyLengthsBuf, sv2, page);
    }
  }
}
