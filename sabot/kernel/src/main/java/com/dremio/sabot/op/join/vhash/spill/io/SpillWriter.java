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
package com.dremio.sabot.op.join.vhash.spill.io;

import static com.dremio.sabot.op.common.ht2.LBlockHashTable.VAR_LENGTH_SIZE;
import static com.dremio.sabot.op.common.ht2.LBlockHashTable.VAR_OFFSET_SIZE;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.op.common.ht2.Copier;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.vhash.spill.SV2UnsignedUtil;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.slicer.PageBatchSlicer;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

/**
 * Spill an incoming batch of records, some of the columns are pivoted and the rest, unpivoted.
 * The writer should not allocate any additional memory, uses 2 pages from the pool.
 * TODO: the page pool must be inited with min 2 pages : one for pivoted, one for non-pivoted
 */
public class SpillWriter implements AutoCloseable {
  private final SpillManager spillManager;
  private final SpillSerializable serializable;
  private final String fileName;
  private final PagePool pagePool;
  private final long sv2;
  private final FixedBlockVector fixed;
  private final VariableBlockVector var;
  private final PageBatchSlicer slicer;

  private SpillManager.SpillOutputStream outputStream;
  private SpillManager.SpillFile spillFile;

  public SpillWriter(SpillManager spillManager, SpillSerializable serializable,
                     String fileName, PagePool pagePool,
                     long sv2, VectorAccessible input, ImmutableBitSet unpivotedColumns,
                     FixedBlockVector fixed, VariableBlockVector var) {
    this.spillManager = spillManager;
    this.serializable = serializable;
    this.fileName = fileName;
    this.pagePool = pagePool;
    this.sv2 = sv2;
    this.fixed = fixed;
    this.var = var;
    this.slicer = new PageBatchSlicer(pagePool, sv2, input, unpivotedColumns);
  }

  public void writeBatch(int records) throws Exception  {
    if (outputStream == null) {
      spillFile = spillManager.getSpillFile(fileName);
      outputStream = spillFile.create(true);
    }

    int recordsDone = 0;
    while (recordsDone < records) {
      try (Page pivotedPage = pagePool.newPage(); Page unpivotedPage = pagePool.newPage()) {
        // count pivoted records that will fit into a page.
        int pickedRecords = pickMaxPivotedRecordsForPage(pivotedPage.getPageSize(), recordsDone, records - 1);
        Preconditions.checkState(pickedRecords > 0);

        // copy unpivoted records that will fit into a page.
        try (RecordBatchData batchData = slicer.copyToPageTillFull(unpivotedPage, recordsDone, recordsDone + pickedRecords - 1)) {
          pickedRecords = batchData.getRecordCount(); // the slicer can pick lesser records than asked.
          Preconditions.checkState(pickedRecords > 0);

          ArrowBuf[] dstBufs = copyPivoted(pivotedPage, recordsDone, recordsDone + pickedRecords - 1);
          try (SpillChunk chunk = new SpillChunk(pickedRecords, dstBufs[0], dstBufs[1], batchData.getContainer(), ImmutableList.of())) {
            serializable.writeChunkToStream(chunk, outputStream);
            recordsDone += batchData.getRecordCount();
          }
        }
      }
    }
  }

  public SpillManager.SpillFile getSpillFile() {
    return spillFile;
  }

  private int pickMaxPivotedRecordsForPage(int availableSize, int startIdx, int endIdx) {
    int maxRecords = endIdx - startIdx + 1;
    if (var.getVariableFieldCount() == 0) {
      // fast-path
      return Integer.min(maxRecords, availableSize / fixed.getBlockWidth());
    }

    int numRecordsPicked = 0;
    long totalSize = 0;
    while (numRecordsPicked < maxRecords) {
      final long recordFixedSize = fixed.getBlockWidth();
      if (totalSize + recordFixedSize > availableSize) {
        // cannot include this record in the page.
        break;
      }

      final int keyIndex = SV2UnsignedUtil.read(sv2, startIdx + numRecordsPicked);
      final long keyFixedAddr = fixed.getMemoryAddress() + ((long) fixed.getBlockWidth() * keyIndex);
      final long keyVarAddr = var.getMemoryAddress() + PlatformDependent.getInt(keyFixedAddr + fixed.getBlockWidth() - VAR_OFFSET_SIZE);
      final long recordVariableSize = PlatformDependent.getInt(keyVarAddr);
      if (totalSize + recordFixedSize + recordVariableSize + VAR_LENGTH_SIZE > availableSize) {
        // cannot include this record in the page.
        break;
      }

      totalSize += (recordFixedSize + recordVariableSize + VAR_LENGTH_SIZE);
      ++numRecordsPicked;
    }
    return numRecordsPicked;
  }

  private ArrowBuf[] copyPivoted(Page dstPage, int startIdx, int endIdx) {
    int numRecords = endIdx - startIdx + 1;

    long startDstFixedAddr = dstPage.getAddress();
    long curDstFixedAddr = startDstFixedAddr;
    // the var section starts after the fixed section
    long startDstVarAddr = startDstFixedAddr + (long) fixed.getBlockWidth() * numRecords;
    long curDstVarAddr = startDstVarAddr;
    for (int i = 0; i < numRecords; ++i) {
      final int keyIndex = SV2UnsignedUtil.read(sv2, startIdx + i);

      // copy fixed section from src to dst
      long curSrcFixedAddr = fixed.getMemoryAddress() + ((long) fixed.getBlockWidth() * keyIndex);
      Copier.copy(curSrcFixedAddr, curDstFixedAddr, fixed.getBlockWidth());
      curSrcFixedAddr += fixed.getBlockWidth();
      curDstFixedAddr += fixed.getBlockWidth();

      if (var.getVariableFieldCount() > 0) {
        final long curSrcVarAddr = var.getMemoryAddress() + PlatformDependent.getInt(curSrcFixedAddr - VAR_OFFSET_SIZE);
        final int recordVarLen = PlatformDependent.getInt(curSrcVarAddr);

        // store the relative offset of the var section in the fixed block
        PlatformDependent.putInt(curDstFixedAddr - VAR_OFFSET_SIZE, (int) (curDstVarAddr - startDstVarAddr));

        // copy the var section from src to dst
        Copier.copy(curSrcVarAddr, curDstVarAddr, recordVarLen + VAR_LENGTH_SIZE);
        curDstVarAddr += (recordVarLen + VAR_LENGTH_SIZE);
      }
    }
    return new ArrowBuf[]{
      dstPage.slice((int) (curDstFixedAddr - startDstFixedAddr)),
      dstPage.slice((int) (curDstVarAddr - startDstVarAddr))
    };
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(outputStream);
  }
}
