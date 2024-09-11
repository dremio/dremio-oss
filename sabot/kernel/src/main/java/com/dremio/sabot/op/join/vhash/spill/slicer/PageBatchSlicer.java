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
package com.dremio.sabot.op.join.vhash.spill.slicer;

import static com.dremio.sabot.op.join.vhash.spill.slicer.Sizer.BYTE_SIZE_BITS;

import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.memory.FormattingUtils;
import com.dremio.sabot.op.aggregate.vectorized.VariableLengthValidator;
import com.dremio.sabot.op.join.vhash.spill.pool.Page;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.join.vhash.spill.pool.PageSlice;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Accumulates a batch stream into a collection of pages, slicing and packing the data as it
 * arrives.
 */
public class PageBatchSlicer {
  static final boolean TRACE = false;

  /** The targeted number of values to step. */
  private static final int STEP = 8;

  private final PagePool pool;
  private ArrowBuf sv2;
  private final Sizer sizer;
  private Page currentPage;

  public PageBatchSlicer(PagePool pool, ArrowBuf sv2, VectorAccessible incoming) {
    this(pool, sv2, incoming, null);
  }

  public PageBatchSlicer(
      PagePool pool, ArrowBuf sv2, VectorAccessible incoming, ImmutableBitSet includedColumns) {
    Preconditions.checkArgument(
        incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE);
    this.pool = pool;
    this.sv2 = sv2;
    List<Sizer> sizerList = new ArrayList<>();

    int index = 0;
    for (VectorWrapper<?> vectorWrapper : incoming) {
      if (includedColumns == null || includedColumns.get(index)) {
        sizerList.add(Sizer.get(vectorWrapper.getValueVector()));
      }
      ++index;
    }
    this.sizer = new CombinedSizer(sizerList);
  }

  public void updateSv2(ArrowBuf sv2) {
    this.sv2 = sv2;
  }

  /**
   * Attempt to add current batch to the internal list of batches.
   *
   * <p>TODO: we can make this more efficient by starting at an estimated midpoint and then moving
   * backwards or forwards as necessary.
   *
   * @return number of added records
   */
  public int addBatch(int records, List<RecordBatchPage> batches) {
    List<PagePlan> plans;

    while (true) {
      LatePageSupplier supplier = new LatePageSupplier(currentPage, pool.getPageSize());
      plans = generatePagePlan(supplier, records);
      if (supplier.tryAllocate(pool)) {
        // successful allocation
        break;
      }

      // trim the records, and retry with a smaller set
      PagePlan last = plans.remove(plans.size() - 1);
      records -= last.count;
      if (records == 0) {
        // cannot trim any further
        return 0;
      }
    }

    batches.clear();
    Page prevPage = currentPage;
    for (PagePlan pp : plans) {
      RecordBatchPage rbp = pp.copy();
      batches.add(rbp);
      currentPage = rbp.getPage();
      if (currentPage != prevPage) {
        // This is the first time we are seeing this page. The RecordBatchPage has a ref on
        // this page, so it's safe to release the ref obtained as part of the page allocation.
        currentPage.release();
      }
      prevPage = currentPage;
    }
    return records;
  }

  /**
   * Copy data from the SV2 to the page till it gets full.
   *
   * @param page page to copy to
   * @param startIdx start index in sv2
   * @param maxIdx end index in sv2
   * @return RecordBatch of copied data
   */
  public RecordBatchPage copyToPageTillFull(Page page, int startIdx, int maxIdx) {
    PageFitResult fitResult =
        countRecordsToFitInPage(
            page.getRemainingBytes() * BYTE_SIZE_BITS, sv2, startIdx, maxIdx - startIdx + 1);
    PagePlan plan =
        new PagePlan(
            fitResult.sizeBits,
            sv2,
            startIdx,
            fitResult.recordCount,
            new SupplierWithCapacity(page));
    return plan.copy();
  }

  private List<PagePlan> generatePagePlan(LatePageSupplier supplier, final int totalRecordCount) {

    List<PagePlan> plans = new ArrayList<>();

    int startIdx = 0;
    int remaining = totalRecordCount;

    sizer.reset();
    while (remaining > 0) { // a loop per page.

      // target the lesser of either the estimated count or the actual record count.
      SupplierWithCapacity current = supplier.getNextPage();
      PageFitResult fitResult =
          countRecordsToFitInPage(current.getRemainingBits(), sv2, startIdx, remaining);

      // if we haven't been able to add any data to this page and there is no data in the page, we
      // have to fail..
      if (fitResult.recordCount == 0 && !current.isPartial()) {
        int required = sizer.computeBitsNeeded(sv2, startIdx, 1);
        throw new IllegalStateException(
            String.format(
                "Individual record size (%s) is too large to fit into page size (%s). "
                    + "Please increase the support option exec.op.join.spill.page_size accordingly.",
                FormattingUtils.formatBytes(required / BYTE_SIZE_BITS),
                FormattingUtils.formatBytes(current.getRemainingBits() / BYTE_SIZE_BITS)));
      }

      if (fitResult.recordCount != 0) {
        // We added data until we filled up this page and have data remaining. Move to the rack and
        // move to the next step.
        plans.add(new PagePlan(fitResult.sizeBits, sv2, startIdx, fitResult.recordCount, current));
      }

      startIdx += fitResult.recordCount;
      remaining -= fitResult.recordCount;
    }
    return plans;
  }

  private PageFitResult countRecordsToFitInPage(
      int availableBits, ArrowBuf sv2, int startIdx, int remaining) {
    sizer.reset();
    int recordSize = sizer.getEstimatedRecordSizeInBits();

    int recordCount = Math.min(recordSize == 0 ? remaining : availableBits / recordSize, remaining);
    int sizeBits = sizer.computeBitsNeeded(sv2, startIdx, recordCount);

    // step until we can't take a full step.
    while (sizeBits < availableBits && recordCount < remaining) {
      recordCount = Math.min(recordCount + STEP, remaining);
      sizeBits = sizer.computeBitsNeeded(sv2, startIdx, recordCount);
    }

    // move backwards until we get within allowed bits.
    while (sizeBits > availableBits && recordCount > 0) {
      recordCount -= 1;
      sizeBits = sizer.computeBitsNeeded(sv2, startIdx, recordCount);
    }

    return new PageFitResult(recordCount, sizeBits);
  }

  private static class PageFitResult {
    final int recordCount;
    final int sizeBits;

    PageFitResult(int recordCount, int sizeBits) {
      this.recordCount = recordCount;
      this.sizeBits = sizeBits;
    }
  }

  /**
   * The plan for copying a range of records from a one batch into a one page (may use all or part
   * of the page).
   */
  private class PagePlan {
    private final SupplierWithCapacity pageSupplier;
    private final List<FieldVector> output = new ArrayList<>();
    private final Copier copier;
    private final int startIdx;
    private final int count;
    private final int expectedBits;

    public PagePlan(
        int expectedBits,
        ArrowBuf sv2,
        int startIdx,
        int count,
        SupplierWithCapacity pageSupplier) {
      this.expectedBits = expectedBits;
      this.copier = sizer.getCopier(pool.getAllocator(), sv2, startIdx, count, output);
      this.pageSupplier = pageSupplier;
      this.startIdx = startIdx;
      this.count = count;
    }

    public RecordBatchPage copy() {
      final Page page = pageSupplier.getPage();
      try (Page capped = new PageSlice(page, expectedBits / BYTE_SIZE_BITS)) {
        if (TRACE) {
          int beforeBytes = page.getRemainingBytes();
          System.out.println(
              "Expected bits: "
                  + expectedBits
                  + ", available bits: "
                  + page.getRemainingBytes() * BYTE_SIZE_BITS);
          copier.copy(capped);
          int actual = (beforeBytes - page.getRemainingBytes()) * BYTE_SIZE_BITS;
          if (actual > expectedBits) {
            System.out.print("! ");
          } else if (actual == expectedBits) {
            System.out.print("= ");
          } else {
            System.out.print("< ");
          }
          System.out.println(
              "Expected bits: "
                  + expectedBits
                  + ", actual bits: "
                  + (beforeBytes - page.getRemainingBytes()) * BYTE_SIZE_BITS);
        } else {
          copier.copy(capped);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      // do validations
      RecordBatchPage data = new RecordBatchPage(count, output, page);
      StreamSupport.stream(data.getContainer().spliterator(), false)
          .map(v -> ((FieldVector) v.getValueVector()))
          .filter(v -> (v instanceof BaseVariableWidthVector))
          .forEach(v -> VariableLengthValidator.validateVariable(v, count));
      // Useful for debugging, but has cost.
      // VectorValidator.validate(data.getContainer());
      return data;
    }

    @Override
    public String toString() {
      return startIdx + ":" + startIdx + count;
    }
  }

  /**
   * Produces page suppliers as they are needed but waits to do allocation until the end. There is
   * minor complexity here since we're trying to avoid duplicating the evaluation of what to include
   * in each copy but not doing any work unless we know we have enough space.
   */
  private static class LatePageSupplier {
    private final Page partialPage;
    private final int pageSize;
    private final List<Page> pages = new ArrayList<>();
    private int newPagesNeeded = 0;
    private boolean allocated;

    public LatePageSupplier(Page partialPage, int pageSize) {
      this.partialPage = partialPage;
      this.pageSize = pageSize;
    }

    public SupplierWithCapacity getNextPage() {
      // if we have some remainder on the current page, let's use that first.
      if (partialPage != null && pages.isEmpty()) {
        pages.add(partialPage);
        return new SupplierWithCapacity(partialPage);
      }

      int pageIndex = pages.size() + newPagesNeeded;
      newPagesNeeded++;
      return new SupplierWithCapacity(() -> pages.get(pageIndex), pageSize);
    }

    /**
     * Attempt to allocate the pages requested. Either completes entirely or fails entirely (returns
     * with no allocations done).
     *
     * @return True if successful. False if allocation failed.
     */
    public boolean tryAllocate(PagePool pool) {
      Preconditions.checkArgument(!allocated);
      List<Page> newPages = pool.getPages(newPagesNeeded);
      if (newPages == null) {
        return false;
      }

      allocated = true;
      pages.addAll(newPages);
      return true;
    }
  }

  /**
   * Holds a future reference to a page along with knowledge of the capacity of that future page.
   */
  private static class SupplierWithCapacity {
    private final Supplier<Page> supplier;

    private final int totalBits;
    private final int remainingBits;

    /**
     * Create supplier with an existing, partially used page.
     *
     * @param existing page
     */
    public SupplierWithCapacity(Page existing) {
      this.totalBits = existing.getPageSize() * BYTE_SIZE_BITS;
      this.remainingBits = existing.getRemainingBytes() * BYTE_SIZE_BITS;
      this.supplier = () -> existing;
    }

    /**
     * Create with a supplier of the new page.
     *
     * @param supplier page supplier
     * @param pageSize page size
     */
    public SupplierWithCapacity(Supplier<Page> supplier, int pageSize) {
      super();
      this.totalBits = pageSize * BYTE_SIZE_BITS;
      this.remainingBits = totalBits;
      this.supplier = supplier;
    }

    public Page getPage() {
      return supplier.get();
    }

    public int getRemainingBits() {
      return remainingBits;
    }

    boolean isPartial() {
      return totalBits != remainingBits;
    }
  }
}
