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
package com.dremio.sabot.op.sort.external;

import static com.dremio.sabot.CustomGenerator.ID;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.TestTools;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.CustomGenerator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.exec.context.OperatorStats;

public class TestSortOp extends BaseTestOperator {

  private BufferAllocator allocator;
  private BufferManager bufferManager;
  private CustomGenerator generator;
  private ClassProducer producer;

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(400, TimeUnit.SECONDS);

  @Before
  public void prepare() {
    allocator = getTestAllocator().newChildAllocator("test-memory-run", 0, 1_000_000);
    bufferManager = new BufferManagerImpl(allocator);
    producer = testContext.newClassProducer(bufferManager);
    generator = new CustomGenerator(20000, getTestAllocator());
  }

  @After
  public void cleanup() throws Exception {
    AutoCloseables.close(allocator, bufferManager, generator);
  }

  @Test
  public void testQuickSorterSpillSortWithUserException1() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 4000);
    } catch (UserException uex) {
      assertEquals("DiskRunManager: Unable to secure enough memory to merge spilled sort data.", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size (in bytes) 236000", uex.getContextStrings().get(2));
      assertEquals("Target Batch Size 4000", uex.getContextStrings().get(3));
      assertEquals(34, uex.getContextStrings().size());
    }
  }

  @Test
  public void testQuickSorterSpillSortWithUserException1WithDirectWriteOff() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false);
         AutoCloseable directWriteOff = with(ExecConstants.EXTERNAL_SORT_DIRECT_WRITE, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 4000);
    } catch (UserException uex) {
      assertEquals("DiskRunManager: Unable to secure enough memory to merge spilled sort data.", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size (in bytes) 236000", uex.getContextStrings().get(2));
      assertEquals("Target Batch Size 4000", uex.getContextStrings().get(3));
      assertEquals(34, uex.getContextStrings().size());
    }
  }

  @Test
  public void testQuickSorterSpillSortWithUserException1WithLegacyEncoding() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false);
         AutoCloseable legacyEncoding = with(ExecConstants.EXTERNAL_SORT_ARROW_ENCODING, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 4000);
    } catch (UserException uex) {
      assertEquals("DiskRunManager: Unable to secure enough memory to merge spilled sort data.", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size (in bytes) 236000", uex.getContextStrings().get(2));
      assertEquals("Target Batch Size 4000", uex.getContextStrings().get(3));
      assertEquals(34, uex.getContextStrings().size());
    }
  }

  @Test
  public void testQuickSorterSpillSortWithUserException2() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(1_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 10000);
    } catch (UserException uex) {
      assertEquals("Memory failed due to not enough memory to sort even one batch of records.", uex.getContextStrings().get(0));
      assertEquals("Target Batch Size (in bytes) 590000", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size 10000", uex.getContextStrings().get(2));
      assertEquals(33, uex.getContextStrings().size());
    }
  }

  @Test
  public void testQuickSorterSpillSortWithUserException2WithDirectWriteOff() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false);
         AutoCloseable directWriteOff = with(ExecConstants.EXTERNAL_SORT_DIRECT_WRITE, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(1_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 10000);
    } catch (UserException uex) {
      assertEquals("Memory failed due to not enough memory to sort even one batch of records.", uex.getContextStrings().get(0));
      assertEquals("Target Batch Size (in bytes) 590000", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size 10000", uex.getContextStrings().get(2));
      assertEquals(33, uex.getContextStrings().size());
    }
  }

  @Test
  public void testQuickSorterSpillSortWithUserException2WithLegacyEncoding() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false);
         AutoCloseable legacyEncoding = with(ExecConstants.EXTERNAL_SORT_ARROW_ENCODING, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(1_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 10000);
    } catch (UserException uex) {
      assertEquals("Memory failed due to not enough memory to sort even one batch of records.", uex.getContextStrings().get(0));
      assertEquals("Target Batch Size (in bytes) 590000", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size 10000", uex.getContextStrings().get(2));
      assertEquals(33, uex.getContextStrings().size());
    }
  }

  @Test
  public void testQuickSorterSpillSort() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 1000);
    }
  }

  @Test
  public void testQuickSorterSpillSortWithDirectWriteOff() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false);
         AutoCloseable directWriteOff = with(ExecConstants.EXTERNAL_SORT_DIRECT_WRITE, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 1000);
    }
  }

  @Test
  public void testQuickSorterSpillSortWithLegacyEncoding() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, false);
         AutoCloseable legacyEncoding = with(ExecConstants.EXTERNAL_SORT_ARROW_ENCODING, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 1000);
    }
  }

  @Test
  public void testMicroSpillBatches() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL, true)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)

      final int numRows = 20_000;
      final int targetBatchSize = 1000;
      final CustomGenerator localGen = new CustomGenerator(numRows, getTestAllocator());
      Fixtures.Table table = localGen.getExpectedSortedTable();
      OperatorStats stats = validateSingle(sort, ExternalSortOperator.class, localGen, table, targetBatchSize);

      final long batchesSpilled = stats.getLongStat(ExternalSortOperator.Metric.BATCHES_SPILLED);
      final long spillFiles = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COUNT);
      final long spilledData = stats.getLongStat(ExternalSortOperator.Metric.TOTAL_SPILLED_DATA_SIZE);
      final long uncompressedBytesRead = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long uncompressedBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long ioBytesRead = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_READ);
      final long ioBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_WRITTEN);
      final long compressionTime = stats.getLongStat(ExternalSortOperator.Metric.COMPRESSION_NANOS);
      final long decompressionTime = stats.getLongStat(ExternalSortOperator.Metric.DECOMPRESSION_NANOS);
      final long readIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_READ_WAIT_NANOS);
      final long writeIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_WRITE_WAIT_NANOS);
      final long spillCopyTime = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COPY_NANOS);

      assertEquals(batchesSpilled, 20 /*numRows/targetBatchSize*/);
      assertEquals(spillFiles, 2);
      assertTrue(spilledData > 0);
      assertTrue(uncompressedBytesRead > 0);
      assertTrue(uncompressedBytesWritten > 0);
      assertTrue(ioBytesRead > 0);
      assertTrue(ioBytesWritten > 0);
      assertTrue(compressionTime > 0);
      assertTrue(decompressionTime > 0);
      assertTrue(readIOWaitTime > 0);
      assertTrue(writeIOWaitTime > 0);
      assertTrue(spillCopyTime > 0);
    }
  }

  @Test
  public void testMicroSpillBatchesWithDirectWriteOff() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL, true);
         AutoCloseable directWriteOff = with(ExecConstants.EXTERNAL_SORT_DIRECT_WRITE, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)

      final int numRows = 20_000;
      final int targetBatchSize = 1000;
      final CustomGenerator localGen = new CustomGenerator(numRows, getTestAllocator());
      Fixtures.Table table = localGen.getExpectedSortedTable();
      OperatorStats stats = validateSingle(sort, ExternalSortOperator.class, localGen, table, targetBatchSize);

      final long batchesSpilled = stats.getLongStat(ExternalSortOperator.Metric.BATCHES_SPILLED);
      final long spillFiles = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COUNT);
      final long spilledData = stats.getLongStat(ExternalSortOperator.Metric.TOTAL_SPILLED_DATA_SIZE);
      final long uncompressedBytesRead = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long uncompressedBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long ioBytesRead = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_READ);
      final long ioBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_WRITTEN);
      final long compressionTime = stats.getLongStat(ExternalSortOperator.Metric.COMPRESSION_NANOS);
      final long decompressionTime = stats.getLongStat(ExternalSortOperator.Metric.DECOMPRESSION_NANOS);
      final long readIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_READ_WAIT_NANOS);
      final long writeIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_WRITE_WAIT_NANOS);
      final long spillCopyTime = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COPY_NANOS);

      assertEquals(batchesSpilled, 20 /*numRows/targetBatchSize*/);
      assertEquals(spillFiles, 2);
      assertTrue(spilledData > 0);
      assertTrue(uncompressedBytesRead > 0);
      assertTrue(uncompressedBytesWritten > 0);
      assertTrue(ioBytesRead > 0);
      assertTrue(ioBytesWritten > 0);
      assertTrue(compressionTime > 0);
      assertTrue(decompressionTime > 0);
      assertTrue(readIOWaitTime > 0);
      assertTrue(writeIOWaitTime > 0);
      assertTrue(spillCopyTime > 0);
    }
  }

  @Test
  public void testMicroSpillBatchesWithLegacyEncoding() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL, true);
         AutoCloseable legacyEncoding = with(ExecConstants.EXTERNAL_SORT_ARROW_ENCODING, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)

      final int numRows = 20_000;
      final int targetBatchSize = 1000;
      final CustomGenerator localGen = new CustomGenerator(numRows, getTestAllocator());
      Fixtures.Table table = localGen.getExpectedSortedTable();
      OperatorStats stats =  validateSingle(sort, ExternalSortOperator.class, localGen, table, targetBatchSize);

      final long batchesSpilled = stats.getLongStat(ExternalSortOperator.Metric.BATCHES_SPILLED);
      final long spillFiles = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COUNT);
      final long spilledData = stats.getLongStat(ExternalSortOperator.Metric.TOTAL_SPILLED_DATA_SIZE);
      final long uncompressedBytesRead = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long uncompressedBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long ioBytesRead = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_READ);
      final long ioBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_WRITTEN);
      final long compressionTime = stats.getLongStat(ExternalSortOperator.Metric.COMPRESSION_NANOS);
      final long decompressionTime = stats.getLongStat(ExternalSortOperator.Metric.DECOMPRESSION_NANOS);
      final long readIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_READ_WAIT_NANOS);
      final long writeIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_WRITE_WAIT_NANOS);
      final long spillCopyTime = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COPY_NANOS);

      assertEquals(batchesSpilled, 20 /*numRows/targetBatchSize*/);
      assertEquals(spillFiles, 2);
      assertTrue(spilledData > 0);
      assertTrue(uncompressedBytesRead > 0);
      assertTrue(uncompressedBytesWritten > 0);
      assertTrue(ioBytesRead > 0);
      assertTrue(ioBytesWritten > 0);
      assertTrue(compressionTime > 0);
      assertTrue(decompressionTime > 0);
      assertTrue(readIOWaitTime > 0);
      assertTrue(writeIOWaitTime > 0);
      assertTrue(spillCopyTime > 0);
    }
  }

  @Test
  public void testMicroSpillBatchesWithGeneratedCopier() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL, true);
         AutoCloseable generatedCopier = with(ExecConstants.EXTERNAL_SORT_VECTOR_COPIER, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)

      final int numRows = 20_000;
      final int targetBatchSize = 1000;
      final CustomGenerator localGen = new CustomGenerator(numRows, getTestAllocator());
      Fixtures.Table table = localGen.getExpectedSortedTable();
      OperatorStats stats =  validateSingle(sort, ExternalSortOperator.class, localGen, table, targetBatchSize);

      final long batchesSpilled = stats.getLongStat(ExternalSortOperator.Metric.BATCHES_SPILLED);
      final long spillFiles = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COUNT);
      final long spilledData = stats.getLongStat(ExternalSortOperator.Metric.TOTAL_SPILLED_DATA_SIZE);
      final long uncompressedBytesRead = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long uncompressedBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long ioBytesRead = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_READ);
      final long ioBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_WRITTEN);
      final long compressionTime = stats.getLongStat(ExternalSortOperator.Metric.COMPRESSION_NANOS);
      final long decompressionTime = stats.getLongStat(ExternalSortOperator.Metric.DECOMPRESSION_NANOS);
      final long readIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_READ_WAIT_NANOS);
      final long writeIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_WRITE_WAIT_NANOS);
      final long spillCopyTime = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COPY_NANOS);

      assertEquals(batchesSpilled, 20 /*numRows/targetBatchSize*/);
      assertEquals(spillFiles, 2);
      assertTrue(spilledData > 0);
      assertTrue(uncompressedBytesRead > 0);
      assertTrue(uncompressedBytesWritten > 0);
      assertTrue(ioBytesRead > 0);
      assertTrue(ioBytesWritten > 0);
      assertTrue(compressionTime > 0);
      assertTrue(decompressionTime > 0);
      assertTrue(readIOWaitTime > 0);
      assertTrue(writeIOWaitTime > 0);
      assertTrue(spillCopyTime > 0);
    }
  }

  @Test
  public void testMicroSpillDisabled() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)

      final int numRows = 20_000;
      final int targetBatchSize = 1000;
      final CustomGenerator localGen = new CustomGenerator(numRows, getTestAllocator());
      Fixtures.Table table = localGen.getExpectedSortedTable();
      OperatorStats stats = validateSingle(sort, ExternalSortOperator.class, localGen, table, targetBatchSize);

      final long batchesSpilled = stats.getLongStat(ExternalSortOperator.Metric.BATCHES_SPILLED);
      final long spillFiles = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COUNT);
      final long spilledData = stats.getLongStat(ExternalSortOperator.Metric.TOTAL_SPILLED_DATA_SIZE);
      final long uncompressedBytesRead = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long uncompressedBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long ioBytesRead = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_READ);
      final long ioBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_WRITTEN);
      final long compressionTime = stats.getLongStat(ExternalSortOperator.Metric.COMPRESSION_NANOS);
      final long decompressionTime = stats.getLongStat(ExternalSortOperator.Metric.DECOMPRESSION_NANOS);
      final long readIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_READ_WAIT_NANOS);
      final long writeIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_WRITE_WAIT_NANOS);
      final long spillCopyTime = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COPY_NANOS);

      assertEquals(batchesSpilled, 0);
      assertEquals(spillFiles, 2);
      assertTrue(spilledData > 0);
      assertTrue(uncompressedBytesRead > 0);
      assertTrue(uncompressedBytesWritten > 0);
      assertTrue(ioBytesRead > 0);
      assertTrue(ioBytesWritten > 0);
      assertTrue(compressionTime > 0);
      assertTrue(decompressionTime > 0);
      assertTrue(readIOWaitTime > 0);
      assertTrue(writeIOWaitTime > 0);
      assertTrue(spillCopyTime > 0);
    }
  }

  @Test
  public void testMicroSpillDisabledWithDirectWriteOff() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL, false);
         AutoCloseable directWriteOff = with(ExecConstants.EXTERNAL_SORT_DIRECT_WRITE, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)

      final int numRows = 20_000;
      final int targetBatchSize = 1000;
      final CustomGenerator localGen = new CustomGenerator(numRows, getTestAllocator());
      Fixtures.Table table = localGen.getExpectedSortedTable();
      OperatorStats stats = validateSingle(sort, ExternalSortOperator.class, localGen, table, targetBatchSize);

      final long batchesSpilled = stats.getLongStat(ExternalSortOperator.Metric.BATCHES_SPILLED);
      final long spillFiles = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COUNT);
      final long spilledData = stats.getLongStat(ExternalSortOperator.Metric.TOTAL_SPILLED_DATA_SIZE);
      final long uncompressedBytesRead = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long uncompressedBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long ioBytesRead = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_READ);
      final long ioBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_WRITTEN);
      final long compressionTime = stats.getLongStat(ExternalSortOperator.Metric.COMPRESSION_NANOS);
      final long decompressionTime = stats.getLongStat(ExternalSortOperator.Metric.DECOMPRESSION_NANOS);
      final long readIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_READ_WAIT_NANOS);
      final long writeIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_WRITE_WAIT_NANOS);
      final long spillCopyTime = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COPY_NANOS);

      assertEquals(batchesSpilled, 0);
      assertEquals(spillFiles, 2);
      assertTrue(spilledData > 0);
      assertTrue(uncompressedBytesRead > 0);
      assertTrue(uncompressedBytesWritten > 0);
      assertTrue(ioBytesRead > 0);
      assertTrue(ioBytesWritten > 0);
      assertTrue(compressionTime > 0);
      assertTrue(decompressionTime > 0);
      assertTrue(readIOWaitTime > 0);
      assertTrue(writeIOWaitTime > 0);
      assertTrue(spillCopyTime > 0);
    }
  }

  @Test
  public void testMicroSpillDisabledWithLegacyEncoding() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL, false);
         AutoCloseable legacyEncoding = with(ExecConstants.EXTERNAL_SORT_ARROW_ENCODING, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)

      final int numRows = 20_000;
      final int targetBatchSize = 1000;
      final CustomGenerator localGen = new CustomGenerator(numRows, getTestAllocator());
      Fixtures.Table table = localGen.getExpectedSortedTable();
      OperatorStats stats =  validateSingle(sort, ExternalSortOperator.class, localGen, table, targetBatchSize);

      final long batchesSpilled = stats.getLongStat(ExternalSortOperator.Metric.BATCHES_SPILLED);
      final long spillFiles = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COUNT);
      final long spilledData = stats.getLongStat(ExternalSortOperator.Metric.TOTAL_SPILLED_DATA_SIZE);
      final long uncompressedBytesRead = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long uncompressedBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long ioBytesRead = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_READ);
      final long ioBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_WRITTEN);
      final long compressionTime = stats.getLongStat(ExternalSortOperator.Metric.COMPRESSION_NANOS);
      final long decompressionTime = stats.getLongStat(ExternalSortOperator.Metric.DECOMPRESSION_NANOS);
      final long readIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_READ_WAIT_NANOS);
      final long writeIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_WRITE_WAIT_NANOS);
      final long spillCopyTime = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COPY_NANOS);

      assertEquals(batchesSpilled, 0);
      assertEquals(spillFiles, 2);
      assertTrue(spilledData > 0);
      assertTrue(uncompressedBytesRead > 0);
      assertTrue(uncompressedBytesWritten > 0);
      assertTrue(ioBytesRead > 0);
      assertTrue(ioBytesWritten > 0);
      assertTrue(compressionTime > 0);
      assertTrue(decompressionTime > 0);
      assertTrue(readIOWaitTime > 0);
      assertTrue(writeIOWaitTime > 0);
      assertTrue(spillCopyTime > 0);
    }
  }

  @Test
  public void testMicroSpillDisabledWithGeneratedCopier() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_MICRO_SPILL, false);
         AutoCloseable generatedCopier = with(ExecConstants.EXTERNAL_SORT_VECTOR_COPIER, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)

      final int numRows = 20_000;
      final int targetBatchSize = 1000;
      final CustomGenerator localGen = new CustomGenerator(numRows, getTestAllocator());
      Fixtures.Table table = localGen.getExpectedSortedTable();
      OperatorStats stats =  validateSingle(sort, ExternalSortOperator.class, localGen, table, targetBatchSize);

      final long batchesSpilled = stats.getLongStat(ExternalSortOperator.Metric.BATCHES_SPILLED);
      final long spillFiles = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COUNT);
      final long spilledData = stats.getLongStat(ExternalSortOperator.Metric.TOTAL_SPILLED_DATA_SIZE);
      final long uncompressedBytesRead = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long uncompressedBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.UNCOMPRESSED_BYTES_READ);
      final long ioBytesRead = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_READ);
      final long ioBytesWritten = stats.getLongStat(ExternalSortOperator.Metric.IO_BYTES_WRITTEN);
      final long compressionTime = stats.getLongStat(ExternalSortOperator.Metric.COMPRESSION_NANOS);
      final long decompressionTime = stats.getLongStat(ExternalSortOperator.Metric.DECOMPRESSION_NANOS);
      final long readIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_READ_WAIT_NANOS);
      final long writeIOWaitTime = stats.getLongStat(ExternalSortOperator.Metric.IO_WRITE_WAIT_NANOS);
      final long spillCopyTime = stats.getLongStat(ExternalSortOperator.Metric.SPILL_COPY_NANOS);

      assertEquals(batchesSpilled, 0);
      assertEquals(spillFiles, 2);
      assertTrue(spilledData > 0);
      assertTrue(uncompressedBytesRead > 0);
      assertTrue(uncompressedBytesWritten > 0);
      assertTrue(ioBytesRead > 0);
      assertTrue(ioBytesWritten > 0);
      assertTrue(compressionTime > 0);
      assertTrue(decompressionTime > 0);
      assertTrue(readIOWaitTime > 0);
      assertTrue(writeIOWaitTime > 0);
      assertTrue(spillCopyTime > 0);
    }
  }

  @Test
  public void testSplayTreeSpillSortWithUserException1() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, true)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(1_500_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 4000);
    } catch (UserException uex) {
      assertEquals("DiskRunManager: Unable to secure enough memory to merge spilled sort data.", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size (in bytes) 236000", uex.getContextStrings().get(2));
      assertEquals("Target Batch Size 4000", uex.getContextStrings().get(3));
      assertEquals(34, uex.getContextStrings().size());
    }
  }

  @Test
  public void testSplayTreeSpillSortWithUserException1WithDirectWriteOff() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, true);
         AutoCloseable directWriteOff = with(ExecConstants.EXTERNAL_SORT_DIRECT_WRITE, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(1_500_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 4000);
    } catch (UserException uex) {
      assertEquals("DiskRunManager: Unable to secure enough memory to merge spilled sort data.", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size (in bytes) 236000", uex.getContextStrings().get(2));
      assertEquals("Target Batch Size 4000", uex.getContextStrings().get(3));
      assertEquals(34, uex.getContextStrings().size());
    }
  }

  @Test
  public void testSplayTreeSpillSortWithUserException1WithLegacyEncoding() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, true);
         AutoCloseable legacyEncoding = with(ExecConstants.EXTERNAL_SORT_ARROW_ENCODING, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(1_500_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 4000);
    } catch (UserException uex) {
      assertEquals("DiskRunManager: Unable to secure enough memory to merge spilled sort data.", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size (in bytes) 236000", uex.getContextStrings().get(2));
      assertEquals("Target Batch Size 4000", uex.getContextStrings().get(3));
      assertEquals(34, uex.getContextStrings().size());
    }
  }

  @Test
  public void testSplayTreeSpillSortWithUserException2() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, true)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(1_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 10000);
    } catch (UserException uex) {
      assertEquals("Memory failed due to not enough memory to sort even one batch of records.", uex.getContextStrings().get(0));
      assertEquals("Target Batch Size (in bytes) 590000", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size 10000", uex.getContextStrings().get(2));
      assertEquals(33, uex.getContextStrings().size());
    }
  }

  @Test
  public void testSplayTreeSpillSortWithUserException2WithDirectWriteOff() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, true);
         AutoCloseable directWriteOff = with(ExecConstants.EXTERNAL_SORT_DIRECT_WRITE, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(1_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 10000);
    } catch (UserException uex) {
      assertEquals("Memory failed due to not enough memory to sort even one batch of records.", uex.getContextStrings().get(0));
      assertEquals("Target Batch Size (in bytes) 590000", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size 10000", uex.getContextStrings().get(2));
      assertEquals(33, uex.getContextStrings().size());
    }
  }

  @Test
  public void testSplayTreeSpillSortWithUserException2WithLegacyEncoding() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, true);
         AutoCloseable legacyEncoding = with(ExecConstants.EXTERNAL_SORT_ARROW_ENCODING, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(1_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 10000);
    } catch (UserException uex) {
      assertEquals("Memory failed due to not enough memory to sort even one batch of records.", uex.getContextStrings().get(0));
      assertEquals("Target Batch Size (in bytes) 590000", uex.getContextStrings().get(1));
      assertEquals("Target Batch Size 10000", uex.getContextStrings().get(2));
      assertEquals(33, uex.getContextStrings().size());
    }
  }

  @Test
  public void testSplayTreeSpillSort() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, true)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 1000);
    }
  }

  @Test
  public void testSplayTreeSpillSortWithDirectWriteOff() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, true);
         AutoCloseable directWriteOff = with(ExecConstants.EXTERNAL_SORT_DIRECT_WRITE, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 1000);
    }
  }

  @Test
  public void testSplayTreeSpillSortWithLegacyEncoding() throws Exception {
    try (AutoCloseable option = with(ExecConstants.EXTERNAL_SORT_ENABLE_SPLAY_SORT, true);
         AutoCloseable legacyEncoding = with(ExecConstants.EXTERNAL_SORT_ARROW_ENCODING, false)) {
      ExternalSort sort = new ExternalSort(PROPS.cloneWithNewReserve(1_000_000), null, singletonList(ordering(ID.getName(), ASCENDING, FIRST)), false);
      sort.getProps().setMemLimit(2_000_000); // this can't go below sort's initialAllocation (20K)
      Fixtures.Table table = generator.getExpectedSortedTable();
      validateSingle(sort, ExternalSortOperator.class, generator, table, 1000);
    }
  }
}
