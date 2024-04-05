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
package com.dremio.sabot.op.common.ht2;

import static org.junit.Assert.assertTrue;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.VectorContainer;
import com.dremio.options.OptionManager;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.test.AllocatorRule;
import com.google.common.base.Stopwatch;
import com.koloboke.collect.hash.HashConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class TestHashTableMultiThread extends BaseTestOperator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestHashTableMultiThread.class);
  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  private final OptionManager options = testContext.getOptions();
  private int MAX_VALUES_PER_BATCH = 4096;
  private int NUM_BATCHES = 40;
  private BufferAllocator allocator;
  private HashTable hashTable;
  private Random rand;
  private ConcurrentHashMap<Integer, Integer> ordinalHashMap;

  Stopwatch timer;

  @Before
  public void before() {
    rand = new Random();
    allocator = allocatorRule.newAllocator("test-hash-table-2", 0, Long.MAX_VALUE);
    ordinalHashMap = new ConcurrentHashMap<>();
    timer = Stopwatch.createStarted();
    populateHT();
    timer.stop();
    System.out.println(
        String.format(
            "Time taken to populate HT with %d batches of size %d:  %d microseconds",
            NUM_BATCHES, MAX_VALUES_PER_BATCH, timer.elapsed(TimeUnit.MICROSECONDS)));
    logger.info(
        "Time taken to populate HT with {} batches of size {} :  {} microseconds ",
        NUM_BATCHES,
        MAX_VALUES_PER_BATCH,
        timer.elapsed(TimeUnit.MICROSECONDS));
    timer.reset();
  }

  @After
  public void after() throws Exception {
    hashTable.close();
    allocator.close();
  }

  @Test
  @Ignore("DX-85599")
  public void testMultiThread_1() throws Exception {
    int numThreads = 25;
    int numIterations = 100;
    int batchSize = 100;
    testMultiThread(numThreads, numIterations, batchSize);
  }

  @Test
  @Ignore("DX-85599")
  public void testMultiThread_2() throws Exception {
    int numThreads = 25;
    int numIterations = 100;
    int batchSize = 4096;
    testMultiThread(numThreads, numIterations, batchSize);
  }

  @Test
  @Ignore("DX-85599")
  public void testMultiThread_3() throws Exception {
    int numThreads = 50;
    int numIterations = 100;
    int batchSize = 100;
    testMultiThread(numThreads, numIterations, batchSize);
  }

  @Test
  @Ignore("DX-85599")
  public void testMultiThread_4() throws Exception {
    int numThreads = 50;
    int numIterations = 100;
    int batchSize = 4096;
    testMultiThread(numThreads, numIterations, batchSize);
  }

  @Test
  public void testSingleThread_1() throws Exception {
    int numThreads = 1;
    int numIterations = 100;
    int batchSize = 100;
    testMultiThread(numThreads, numIterations, batchSize);
  }

  @Test
  public void testSingleThread_2() throws Exception {
    int numThreads = 1;
    int numIterations = 100;
    int batchSize = 4096;
    testMultiThread(numThreads, numIterations, batchSize);
  }

  public void testMultiThread(int numThreads, int numIterations, int batchSize) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    List<Callable<ReadHTable>> tasks = new ArrayList<Callable<ReadHTable>>();
    for (int i = 0; i < numThreads; i++) {
      tasks.add(new ReadHTable(numIterations, batchSize));
    }
    timer.start();
    List<Future<ReadHTable>> futures = executorService.invokeAll(tasks);
    timer.stop();
    for (Future<ReadHTable> future : futures) {
      assert (future.isDone());
    }
    System.out.println(
        String.format(
            "Time taken to read in %d threads with %d iterations of read batch size %d : %d microseconds ",
            numThreads, numIterations, batchSize, timer.elapsed(TimeUnit.MICROSECONDS)));
    logger.info(
        "Time taken to read in {} threads with {} iterations of read batch size {} : {} microseconds ",
        numThreads,
        numIterations,
        batchSize,
        timer.elapsed(TimeUnit.MICROSECONDS));
    executorService.shutdownNow();
  }

  private void populateHT() {
    String[] col1arr = new String[MAX_VALUES_PER_BATCH];
    String[] col2arr = new String[MAX_VALUES_PER_BATCH];
    Integer[] col3arr = new Integer[MAX_VALUES_PER_BATCH];
    int batchRowNum;

    for (int batchNum = 0; batchNum < NUM_BATCHES; batchNum++) {
      for (batchRowNum = 0; batchRowNum < MAX_VALUES_PER_BATCH; batchRowNum++) {
        int randNum = rand.nextInt(300_000);
        col1arr[batchRowNum] = new String("col1_" + randNum);
        col2arr[batchRowNum] = new String("col2_" + randNum);
        col3arr[batchRowNum] = randNum;
      }
      try (final VectorContainer c = new VectorContainer()) {
        VarCharVector col1 = new VarCharVector("col1", allocator);
        TestVarBinaryPivot.populate(col1, col1arr);
        c.add(col1);
        VarCharVector col2 = new VarCharVector("col2", allocator);
        TestVarBinaryPivot.populate(col2, col2arr);
        c.add(col2);
        IntVector col3 = new IntVector("col3", allocator);
        TestIntPivot.populate(col3, col3arr);
        c.add(col3);

        final int records = c.setAllCount(col1arr.length);
        final PivotDef pivot =
            PivotBuilder.getBlockDefinition(
                new FieldVectorPair(col1, col1),
                new FieldVectorPair(col2, col2),
                new FieldVectorPair(col3, col3));

        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector var =
            new VariableBlockVector(allocator, pivot.getVariableCount());

        Pivots.pivot(pivot, records, fbv, var);
        if (hashTable == null) {
          HashTable.HashTableCreateArgs hashTableCreateArgs =
              new HashTable.HashTableCreateArgs(
                  HashConfig.getDefault(),
                  pivot,
                  allocator,
                  16 * 1024,
                  ExecConstants.BATCH_VARIABLE_FIELD_SIZE_ESTIMATE
                      .getDefault()
                      .getNumVal()
                      .intValue(),
                  false,
                  MAX_VALUES_PER_BATCH,
                  null,
                  true);
          hashTable = HashTable.getInstance(SabotConfig.create(), options, hashTableCreateArgs);
        }
        ArrowBuf hashkey8B = allocator.buffer(8 * records);
        hashTable.computeHash(records, fbv.getBuf(), var.getBuf(), 0, hashkey8B);
        long hashKeyBBAddress = hashkey8B.memoryAddress();
        ArrowBuf ordinals = allocator.buffer(4 * records);
        // ArrowBuf needs to be accessed at the right index since it is not strongly typed.
        // For integer, multiply the index by 4
        hashTable.add(records, fbv.getBuf(), var.getBuf(), hashkey8B, ordinals);
        for (int i = 0; i < records; i++) {
          logger.debug(
              "Inserted keyvalue {} Returned Ordinal {} ", col3arr[i], ordinals.getInt(i * 4));
          ordinalHashMap.put(col3arr[i], ordinals.getInt(i * 4));
        }
        hashkey8B.close();
        ordinals.close();
        fbv.close();
        var.close();
      }
    }
  }

  private class ReadHTable implements Callable {
    int numIterations;
    int readBatchSize;

    public ReadHTable(final int numIterations, final int readBatchSize) {
      this.numIterations = numIterations;
      this.readBatchSize = readBatchSize;
    }

    @Override
    public Object call() throws Exception {
      for (int i = 0; i < numIterations; i++) {
        int unmatchedOrdinal = readKeys();
        assertTrue(unmatchedOrdinal == 0);
      }
      return true;
    }

    private int readKeys() {
      int matchedOrdinal = 0;
      int unmatchedOrdinal = 0;
      int notFoundValues = 0;
      String[] col1arr = new String[readBatchSize];
      String[] col2arr = new String[readBatchSize];
      Integer[] col3arr = new Integer[readBatchSize];
      int batchRowNum;

      for (batchRowNum = 0; batchRowNum < col1arr.length; batchRowNum++) {
        int randNum = rand.nextInt(300_000);
        col1arr[batchRowNum] = new String("col1_" + randNum);
        col2arr[batchRowNum] = new String("col2_" + randNum);
        col3arr[batchRowNum] = randNum;
      }
      try (BufferAllocator readAllocator =
              allocator.newChildAllocator("Read-allocator", 0, Long.MAX_VALUE);
          final VectorContainer c = new VectorContainer()) {
        VarCharVector col1 = new VarCharVector("col1", readAllocator);
        TestVarBinaryPivot.populate(col1, col1arr);
        c.add(col1);
        VarCharVector col2 = new VarCharVector("col2", readAllocator);
        TestVarBinaryPivot.populate(col2, col2arr);
        c.add(col2);
        IntVector col3 = new IntVector("col3", readAllocator);
        TestIntPivot.populate(col3, col3arr);
        c.add(col3);

        final int records = c.setAllCount(col1arr.length);
        final PivotDef pivot =
            PivotBuilder.getBlockDefinition(
                new FieldVectorPair(col1, col1),
                new FieldVectorPair(col2, col2),
                new FieldVectorPair(col3, col3));

        try (FixedBlockVector fbv = new FixedBlockVector(readAllocator, pivot.getBlockWidth());
            VariableBlockVector var =
                new VariableBlockVector(readAllocator, pivot.getVariableCount())) {

          Pivots.pivot(pivot, records, fbv, var);
          try (ArrowBuf hashkey8B = readAllocator.buffer(8 * records);
              ArrowBuf ordinals = readAllocator.buffer(4 * records)) {
            hashTable.computeHash(records, fbv.getBuf(), var.getBuf(), 0, hashkey8B);
            hashTable.find(records, fbv.getBuf(), var.getBuf(), hashkey8B, ordinals);
            for (int i = 0; i < records; i++) {
              Integer ordinal = ordinalHashMap.get(col3arr[i]);
              if (ordinal == null) {
                notFoundValues++;
              } else if (ordinal != ordinals.getInt(i * 4)) {
                unmatchedOrdinal++;
                logger.info(
                    "Mismatched values key value: {}, Saved Ordinal {}, New Ordinals {} ",
                    col3arr[i],
                    ordinal,
                    ordinals.getInt(i * 4));
              } else {
                matchedOrdinal++;
              }
            }
          }
        }
      }
      logger.info(
          "Matched Ordinal {}, Unmatched Ordinal {}, NotFoundValues {} ",
          matchedOrdinal,
          unmatchedOrdinal,
          notFoundValues);
      return unmatchedOrdinal;
    }
  }
}
