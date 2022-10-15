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
package com.dremio.sabot.op.join.vhash.spill.partition.io;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.BasePath;
import com.dremio.exec.ExecTest;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Generator;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.Pivots;
import com.dremio.sabot.op.common.ht2.Unpivots;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.vhash.spill.io.BatchCombiningSpillReader;
import com.dremio.sabot.op.join.vhash.spill.io.SpillChunk;
import com.dremio.sabot.op.join.vhash.spill.io.SpillSerializableImpl;
import com.dremio.sabot.op.join.vhash.spill.io.SpillWriter;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.dremio.sabot.op.sort.external.SpillManager;
import com.dremio.service.spill.SpillDirectory;
import com.dremio.service.spill.SpillService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import io.airlift.tpch.GenerationDefinition;
import io.airlift.tpch.TpchGenerator;
import io.netty.util.internal.PlatformDependent;

/**
 * Test the hash-join spill writer/reader code :
 * - Writes batches of records, with some columns pivoted and some non-pivoted
 * - Reads back the same batches & verifies they are the same as those that were written.
 */
public class TestSpillSerDe extends ExecTest {
  private static SpillManager spillManager;

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

    final FileSystem fs = FileSystem.get(conf);
    final File tempDir = Files.createTempDir();
    final Path path = new Path(tempDir.getAbsolutePath(), "dremioSerializable");
    tempDir.deleteOnExit();
    final SpillDirectory spillDirectory = new SpillDirectory(path, fs);

    SpillService spillService = mock(SpillService.class);
    doAnswer(new Answer<SpillDirectory>() {
      @Override
      public SpillDirectory answer(InvocationOnMock invocationOnMock) throws Throwable {
        return spillDirectory;
      }
    }).when(spillService).getSpillSubdir(any(String.class));
    spillManager = new SpillManager(ImmutableList.of(path.getName()), null, "test", spillService, "testSpill", null);
  }

  @AfterClass
  public static void teardown() throws Exception {
    AutoCloseables.close(spillManager);
  }

  @Test
  public void simple() throws Exception {
    check(GenerationDefinition.TpchTable.REGION, 0.1, 64_000, 4095, 1);
  }

  @Test
  public void manyBatches() throws Exception {
    check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, 64_000, 4095, 1);
  }

  @Test
  public void manyBatchesMerge() throws Exception {
    check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, 64_000, 100, 1);
  }

  @Test
  public void manyBatchesMultiplePivotColumns() throws Exception {
    check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, 64_000, 4095, 3);
  }

  @Test
  public void manyBatchesMultiplePivotColumnsMerge() throws Exception {
    check(GenerationDefinition.TpchTable.CUSTOMER, 0.01, 64_000, 100, 3);
  }

  @Test
  public void manyBatchesNoCarryAlong() throws Exception {
    check(GenerationDefinition.TpchTable.CUSTOMER, 0.1, 64_000, 4095, 1, "c_custkey");
  }

  private void check(GenerationDefinition.TpchTable table, double scale, int pageSize, int batchSize, int numPivotColumns, String... columns) throws Exception {
    Fixtures.Table expected = TpchGenerator.singleGenerator(table, scale, getAllocator(), columns).toTable(batchSize);
    check(expected, pageSize, batchSize, numPivotColumns, TpchGenerator.singleGenerator(table, scale, allocator, columns));
  }

  private void check(Fixtures.Table expected, int pageSize, int batchSize, int numPivotColumns, Generator generator) throws Exception {
    final String fileName = "batches";

    try (AutoCloseables.RollbackCloseable rc = new AutoCloseables.RollbackCloseable(true)) {
      rc.add(generator);

      // pivot using first few columns, remaining columns are unpivoted
      List<FieldVectorPair> pivotVectorPairs = new ArrayList<>();
      for (int i = 0; i < numPivotColumns; ++i) {
        Field pivotColumn = generator.getOutput().getSchema().getColumn(i);
        int fieldId = generator.getOutput().getSchema().getFieldId(BasePath.getSimple(pivotColumn.getName())).getFieldIds()[0];
        FieldVector pivotVector = rc.add(generator.getOutput().getValueAccessorById(FieldVector.class, fieldId).getValueVector());
        FieldVector unpivotVector = rc.add(pivotColumn.createVector(allocator));
        pivotVectorPairs.add(new FieldVectorPair(pivotVector, unpivotVector));
      }
      PivotDef pivotDef = PivotBuilder.getBlockDefinition(pivotVectorPairs);

      ImmutableBitSet unpivotedBitSet = ImmutableBitSet.range(numPivotColumns, generator.getOutput().getSchema().getFieldCount());

      final PagePool pool = rc.add(new PagePool(getAllocator(), pageSize, 0));

      // stream data to spill file
      SpillManager.SpillFile spillFile = streamToOutput(fileName, generator, batchSize, pivotDef,  unpivotedBitSet, pool);

      // read back from the spill file
      List<SpillChunk> chunks = readAllFromInput(spillFile, generator.getOutput(), pivotDef, unpivotedBitSet, pool);
      rc.addAll(chunks);

      // The pivoted data is present in the fixed/variable blocks, while the container has the unpivoted data.
      // Combine the two & create actual record batches.
      final List<RecordBatchData> actual = new ArrayList<>();
      chunks.forEach(chunk -> {
        // populate vectors from the pivoted data
        for (FieldVectorPair pair : pivotVectorPairs) {
          AllocationHelper.allocate(pair.getOutgoing(), chunk.getNumRecords(), 15);
        }
        Unpivots.unpivotToAllocedOutput(pivotDef, chunk.getFixed().memoryAddress(), chunk.getVariable().memoryAddress(),
          0, chunk.getNumRecords(), 0);
        for (FieldVectorPair pair : pivotVectorPairs) {
          pair.getOutgoing().setValueCount(chunk.getNumRecords());
        }

        VectorContainer combined = new VectorContainer(allocator);
        // add pivoted vectors to container
        for (FieldVectorPair pair : pivotVectorPairs) {
          combined.add(pair.getOutgoing());
        }
        // add unpivoted vectors to container
        for (VectorWrapper<?> it : chunk.getContainer()) {
          combined.add(it.getValueVector());
        }
        combined.buildSchema();
        combined.setRecordCount(chunk.getNumRecords());

        RecordBatchData batchData = rc.add(new RecordBatchData(combined, getAllocator()));
        actual.add(batchData);
      });

      // compare actual with expected
      expected.checkValid(actual);
    }
  }

  private SpillManager.SpillFile streamToOutput(String fileName, Generator generator, int batchSize, PivotDef pivotDef, ImmutableBitSet unpivotedColumns,
                                                PagePool pool) throws Exception {
    try (ArrowBuf sv2Buf = getFilledSV2(getAllocator(), batchSize);
         FixedBlockVector pivotedFixed = new FixedBlockVector(getAllocator(), pivotDef.getBlockWidth());
         VariableBlockVector pivotedVariable = new VariableBlockVector(getAllocator(), pivotDef.getVariableCount())) {

      try (SpillWriter writer = new SpillWriter(spillManager, new SpillSerializableImpl(), fileName, pool, sv2Buf,
           generator.getOutput(), unpivotedColumns, pivotedFixed, pivotedVariable)) {
        int records;
        while ((records = generator.next(batchSize)) != 0) {
          pivotedFixed.reset();
          pivotedVariable.reset();
          Pivots.pivot(pivotDef, batchSize, pivotedFixed, pivotedVariable);
          writer.writeBatch(0, records);
        }

        return writer.getSpillFileDescriptor().getFile();
      }
    }
  }

  private List<SpillChunk> readAllFromInput(SpillManager.SpillFile spillFile, VectorAccessible incoming,
                                            PivotDef pivotDef, ImmutableBitSet unpivotedColumns, PagePool pool) throws Exception {
    int index = 0;
    List<Field> unpivotedFields = new ArrayList<>();
    for (VectorWrapper<?> vector : incoming) {
      if (unpivotedColumns.get(index)) {
        unpivotedFields.add(vector.getField());
      }
      ++index;
    }

    try (BatchCombiningSpillReader reader = new BatchCombiningSpillReader(spillFile, new SpillSerializableImpl(), pool, pivotDef, new BatchSchema(unpivotedFields), 4095)) {
      return Lists.newArrayList(reader);
    }
  }

  private ArrowBuf getFilledSV2(BufferAllocator allocator, int batchSize) {
    ArrowBuf buf = allocator.buffer((long) SelectionVector2.RECORD_SIZE * batchSize);
    long addr = buf.memoryAddress();
    for (int i = 0; i < batchSize; ++i) {
      PlatformDependent.putShort(addr, (short)i);
      addr += SelectionVector2.RECORD_SIZE;
    }
    return buf;
  }
}
