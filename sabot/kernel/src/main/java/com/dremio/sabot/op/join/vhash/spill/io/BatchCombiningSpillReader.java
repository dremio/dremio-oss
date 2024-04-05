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

import static com.dremio.sabot.op.common.ht2.LBlockHashTable.VAR_OFFSET_SIZE;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.CloseableIterator;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.pool.PageSupplier;
import com.dremio.sabot.op.join.vhash.spill.pool.ReusingPageSupplier;
import com.dremio.sabot.op.join.vhash.spill.slicer.PageBatchMerger;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;

/**
 * Wrapper on top of SpillReader to merge batches that are read. This helps maintain a large
 * batch-size & avoid heap churn.
 */
public class BatchCombiningSpillReader implements CloseableIterator<SpillChunk> {
  private final SpillReader reader;
  private final PagePool pagePool;
  private final PivotDef pivotDef;
  private final PageSupplier pageSupplier;
  private final int maxInputBatchSize;
  private PageBatchMerger merger;

  public BatchCombiningSpillReader(
      SpillManager.SpillFile spillFile,
      SpillSerializable serializable,
      PagePool pagePool,
      PivotDef pivotDef,
      BatchSchema unpivotedColumnsSchema,
      int maxInputBatchSize) {
    this.pageSupplier = new ReusingPageSupplier(pagePool);
    this.reader = new SpillReader(spillFile, serializable, pageSupplier, unpivotedColumnsSchema);
    this.pagePool = pagePool;
    this.pivotDef = pivotDef;
    this.maxInputBatchSize = maxInputBatchSize;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(reader, pageSupplier);
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public SpillChunk next() {
    List<SpillChunk> toMerge = new ArrayList<>();
    int combinedRecords = 0;
    int combinedPivotedBytes = 0;
    int combinedUnpivotedBytes = 0;

    while (reader.hasNext()) {
      // see if this chunk can be merged too.
      SpillChunk peek = reader.peek();
      if (combinedRecords + peek.getNumRecords() > maxInputBatchSize
          || combinedPivotedBytes + peek.getPivotedSizeRounded() > pagePool.getPageSize()
          || combinedUnpivotedBytes + peek.getUnpivotedSizeRounded() > pagePool.getPageSize()) {
        break;
      }

      combinedRecords += peek.getNumRecords();
      combinedPivotedBytes += peek.getPivotedSizeRounded();
      combinedUnpivotedBytes += peek.getUnpivotedSizeRounded();
      toMerge.add(reader.next());
    }
    return mergeChunks(toMerge);
  }

  private SpillChunk mergeChunks(List<SpillChunk> chunks) {
    Preconditions.checkArgument(chunks.size() > 0);
    if (chunks.size() == 1) {
      // trivial case
      return chunks.get(0);
    }

    // merge
    try (AutoCloseables.RollbackCloseable ac = new AutoCloseables.RollbackCloseable(true)) {
      Page pivotedPage = ac.add(pagePool.newPage());
      Page unpivotedPage = ac.add(pagePool.newPage());

      ArrowBuf[] pivotedSections = mergePivotedSections(chunks, pivotedPage);
      ac.addAll(pivotedSections);

      VectorContainer mergedContainer = mergeUnpivotedSections(chunks, unpivotedPage);
      SpillChunk merged =
          new SpillChunk(
              mergedContainer.getRecordCount(),
              pivotedSections[0],
              pivotedSections[1],
              mergedContainer,
              Arrays.asList(pivotedPage, unpivotedPage));
      ac.commit();

      // release the old chunks
      AutoCloseables.close(chunks);
      return merged;
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, RuntimeException.class);
      throw new RuntimeException(e);
    }
  }

  private ArrowBuf[] mergePivotedSections(List<SpillChunk> chunks, Page pivotedPage) {
    int pivotedBlockWidth = pivotDef.getBlockWidth();
    boolean pivotHasVarSection = pivotDef.getVariableCount() > 0;

    int totalFixed = 0;
    int totalVariable = 0;
    for (SpillChunk chunk : chunks) {
      totalFixed += chunk.getFixed().capacity();
      totalVariable += chunk.getVariable().capacity();
    }
    ArrowBuf mergedFixed = pivotedPage.sliceAligned(totalFixed);
    ArrowBuf mergedVariable = pivotedPage.sliceAligned(totalVariable);

    int fixedOffset = 0;
    int variableOffset = 0;
    for (SpillChunk chunk : chunks) {
      // copy fixed portion
      mergedFixed.setBytes(fixedOffset, chunk.getFixed(), 0, chunk.getFixed().capacity());

      if (pivotHasVarSection) {
        for (int i = 0; i < chunk.getNumRecords(); ++i) {
          // the fixed section of each record contains the relative offset of the variable section.
          // shift the relative offset based on the starting offset for this chunk after the merge.
          long varOffsetAddr = fixedOffset + (long) (i + 1) * pivotedBlockWidth - VAR_OFFSET_SIZE;
          int oldOffset = mergedFixed.getInt(varOffsetAddr);
          mergedFixed.setInt(varOffsetAddr, oldOffset + variableOffset);
        }

        // copy variable portion
        mergedVariable.setBytes(
            variableOffset, chunk.getVariable(), 0, chunk.getVariable().capacity());
      }
      fixedOffset += chunk.getFixed().capacity();
      variableOffset += chunk.getVariable().capacity();
    }
    return new ArrowBuf[] {mergedFixed, mergedVariable};
  }

  private VectorContainer mergeUnpivotedSections(List<SpillChunk> chunks, Page unpivotedPage) {
    if (merger == null) {
      merger = new PageBatchMerger(chunks.get(0).getContainer(), pagePool.getAllocator());
    }
    List<VectorContainer> containers = new ArrayList<>();
    for (SpillChunk chunk : chunks) {
      containers.add(chunk.getContainer());
    }
    return merger.merge(containers, unpivotedPage);
  }
}
