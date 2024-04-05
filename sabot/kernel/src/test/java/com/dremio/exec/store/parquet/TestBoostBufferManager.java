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
package com.dremio.exec.store.parquet;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.easy.arrow.ArrowRecordReader;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.spill.DefaultSpillServiceOptions;
import com.dremio.service.spill.SpillService;
import com.dremio.service.spill.SpillServiceImpl;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link BoostBufferManager} */
public class TestBoostBufferManager {
  private BufferAllocator testAllocator;
  private SpillService spillService;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  public TemporaryFolder spillParentDir = new TemporaryFolder();

  public class NoopScheduler implements SchedulerService {
    private Runnable taskToRun;

    @Override
    public Cancellable schedule(Schedule schedule, Runnable task) {
      taskToRun = task;
      return new Cancellable() {
        @Override
        public void cancel(boolean mayInterruptIfRunning) {}

        @Override
        public boolean isCancelled() {
          return true;
        }

        @Override
        public boolean isDone() {
          return false;
        }
      };
    }

    @Override
    public void start() {}

    @Override
    public void close() {}
  }

  class TestSpillServiceOptions extends DefaultSpillServiceOptions {
    // sweep every time the task runs
    @Override
    public long spillSweepInterval() {
      return 0;
    }

    // sweep every single directory every time the task runs
    @Override
    public long spillSweepThreshold() {
      return 0;
    }
  }

  @Before
  public void setupBeforeTest() throws Exception {
    testAllocator = allocatorRule.newAllocator("test-boost-Buffer-manager", 0, Long.MAX_VALUE);
    spillParentDir.create();
    final File spillDir = spillParentDir.newFolder();
    final DremioConfig config = mock(DremioConfig.class);
    when(config.getStringList(DremioConfig.SPILLING_PATH_STRING))
        .thenReturn(ImmutableList.of(spillDir.getPath()));
    final NoopScheduler schedulerService = new NoopScheduler();
    spillService =
        new SpillServiceImpl(
            config,
            new TestSpillServiceOptions(),
            new Provider<SchedulerService>() {
              @Override
              public SchedulerService get() {
                return schedulerService;
              }
            });
    spillService.start();
  }

  private OperatorContext getCtx() {
    OperatorContext operatorContext = mock(OperatorContext.class, RETURNS_DEEP_STUBS);
    when(operatorContext.getAllocator()).thenReturn(testAllocator);
    ExecProtos.FragmentHandle fragmentHandle = ExecProtos.FragmentHandle.getDefaultInstance();
    when(operatorContext.getFragmentHandle()).thenReturn(fragmentHandle);
    when(operatorContext.getSpillService()).thenReturn(spillService);
    when(operatorContext.getNodeEndpointProvider()).thenReturn(null);
    return operatorContext;
  }

  @After
  public void cleanupAfterTest() {
    testAllocator.close();
  }

  @Test
  public void testBoostBufferManagerWithOneBatch() throws Exception {
    BoostBufferManager boostBufferManager =
        spy(
            new BoostBufferManager(
                mock(FragmentExecutionContext.class),
                getCtx(),
                getProps(),
                getTableFunctionConfig()));

    doNothing().when(boostBufferManager).createAndFireBoostQuery();

    List<SplitAndPartitionInfo> splitAndPartitionInfos = buildSplit(7);

    List<SchemaPath> columns = ImmutableList.of(SchemaPath.getSimplePath("column1"));
    splitAndPartitionInfos.stream()
        .forEach(
            x -> {
              try {
                ParquetProtobuf.ParquetDatasetSplitScanXAttr parquetDatasetSplitScanXAttr =
                    LegacyProtobufSerializer.parseFrom(
                        ParquetProtobuf.ParquetDatasetSplitScanXAttr.PARSER,
                        x.getDatasetSplitInfo().getExtendedProperty());
                boostBufferManager.addSplit(x, parquetDatasetSplitScanXAttr, columns);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });

    boostBufferManager.setIcebergColumnIds("TestBytes".getBytes());
    boostBufferManager.close();

    Path arrowFilePath = boostBufferManager.getArrowFilePath();
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    List<Field> fieldList =
        Arrays.asList(
            CompleteType.VARBINARY.toField(BoostBufferManager.SPLITS_VECTOR),
            CompleteType.VARBINARY.toField(BoostBufferManager.COL_IDS));

    BatchSchema schema = new BatchSchema(fieldList);

    try (VectorContainer container = VectorContainer.create(testAllocator, schema);
        ArrowRecordReader reader =
            new ArrowRecordReader(
                getCtx(),
                fs,
                arrowFilePath,
                ImmutableList.of(
                    SchemaPath.getSimplePath(BoostBufferManager.SPLITS_VECTOR),
                    SchemaPath.getSimplePath(BoostBufferManager.COL_IDS)));
        SampleMutator mutator = new SampleMutator(testAllocator)) {
      Field splitInfo = CompleteType.VARBINARY.toField(BoostBufferManager.SPLITS_VECTOR);
      Field colIds = CompleteType.VARBINARY.toField(BoostBufferManager.COL_IDS);
      mutator.addField(splitInfo, VarBinaryVector.class);
      mutator.addField(colIds, VarBinaryVector.class);
      mutator.getContainer().buildSchema();
      reader.allocate(mutator.getFieldVectorMap());
      reader.setup(mutator);

      int totalRecordCount = 0;
      int currentCount = 0;

      // read every thing using arrow record reader. Should read 7 splits.
      while ((currentCount = reader.next()) != 0) {
        totalRecordCount += currentCount;
      }

      assertEquals(totalRecordCount, 7);
      // Only one batch is read so just assert the vectors
      assertEquals(
          "partition_extended_boostBuffer2",
          getPartitionExtendedProp(
              (VarBinaryVector) mutator.getVector(BoostBufferManager.SPLITS_VECTOR), 2));
      assertEquals(
          "partition_extended_boostBuffer5",
          getPartitionExtendedProp(
              (VarBinaryVector) mutator.getVector(BoostBufferManager.SPLITS_VECTOR), 5));
      assertEquals(
          "TestBytes",
          new String(((VarBinaryVector) mutator.getVector(BoostBufferManager.COL_IDS)).get(0)));
    }
  }

  @Test
  public void testBoostBufferManagerWithMultipleBatches() throws Exception {
    BoostBufferManager boostBufferManager =
        spy(
            new BoostBufferManager(
                mock(FragmentExecutionContext.class),
                getCtx(),
                getProps(),
                getTableFunctionConfig()));

    doNothing().when(boostBufferManager).createAndFireBoostQuery();

    // We will write 54 splits with batch size of 10. So we will write 6 batches.
    List<SplitAndPartitionInfo> splitAndPartitionInfos = buildSplit(54);

    List<SchemaPath> columns =
        ImmutableList.of(SchemaPath.getSimplePath(BoostBufferManager.SPLITS_VECTOR));
    boostBufferManager.setBatchSize(10);

    splitAndPartitionInfos.stream()
        .forEach(
            x -> {
              try {
                ParquetProtobuf.ParquetDatasetSplitScanXAttr parquetDatasetSplitScanXAttr =
                    LegacyProtobufSerializer.parseFrom(
                        ParquetProtobuf.ParquetDatasetSplitScanXAttr.PARSER,
                        x.getDatasetSplitInfo().getExtendedProperty());
                boostBufferManager.addSplit(x, parquetDatasetSplitScanXAttr, columns);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });

    boostBufferManager.setIcebergColumnIds("TestBytes".getBytes());
    boostBufferManager.close();
    Path arrowFilePath = boostBufferManager.getArrowFilePath();
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());

    List<Field> fieldList =
        Arrays.asList(
            CompleteType.VARBINARY.toField(BoostBufferManager.SPLITS_VECTOR),
            CompleteType.VARBINARY.toField(BoostBufferManager.COL_IDS));

    BatchSchema schema = new BatchSchema(fieldList);

    try (VectorContainer container = VectorContainer.create(testAllocator, schema);
        ArrowRecordReader reader =
            new ArrowRecordReader(
                getCtx(),
                fs,
                arrowFilePath,
                ImmutableList.of(
                    SchemaPath.getSimplePath(BoostBufferManager.SPLITS_VECTOR),
                    SchemaPath.getSimplePath(BoostBufferManager.COL_IDS)));
        SampleMutator mutator = new SampleMutator(testAllocator)) {
      Field splitInfo = CompleteType.VARBINARY.toField(BoostBufferManager.SPLITS_VECTOR);
      Field colIds = CompleteType.VARBINARY.toField(BoostBufferManager.COL_IDS);
      mutator.addField(splitInfo, VarBinaryVector.class);
      mutator.addField(colIds, VarBinaryVector.class);
      mutator.getContainer().buildSchema();
      reader.allocate(mutator.getFieldVectorMap());
      reader.setup(mutator);

      int totalRecordCount = 0;
      int currentCount = 0;
      int batchesRead = 0;
      // read every thing using arrow record reader. Should read 54 splits.
      while ((currentCount = reader.next()) != 0) {
        totalRecordCount += currentCount;
        // assert the value of first element in each batch read.
        // Batch size will be ten and we would read 6 batches
        assertEquals(
            "partition_extended_boostBuffer" + batchesRead * 10,
            getPartitionExtendedProp(
                (VarBinaryVector) mutator.getVector(BoostBufferManager.SPLITS_VECTOR), 0));
        batchesRead++;
      }
      assertEquals(totalRecordCount, 54);
      assertEquals(batchesRead, 6);
      assertEquals(
          "TestBytes",
          new String(((VarBinaryVector) mutator.getVector(BoostBufferManager.COL_IDS)).get(0)));
    }
  }

  private List<SplitAndPartitionInfo> buildSplit(int num) {
    List<SplitAndPartitionInfo> splitAndPartitionInfos = new ArrayList<>();

    for (int i = 0; i < num; ++i) {
      ParquetProtobuf.ParquetDatasetSplitScanXAttr parquetXAttr =
          ParquetProtobuf.ParquetDatasetSplitScanXAttr.newBuilder()
              .setPath("dummyPath")
              .setLength(0)
              .setStart(0)
              .setFileLength(125)
              .setRowGroupIndex(0)
              .setLastModificationTime(0)
              .build();

      String extendedProp = "partition_extended_boostBuffer" + String.valueOf(i);
      PartitionProtobuf.NormalizedPartitionInfo partitionInfo =
          PartitionProtobuf.NormalizedPartitionInfo.newBuilder()
              .setId(String.valueOf(i))
              .setSize(i * 1000)
              .setExtendedProperty(ByteString.copyFrom(extendedProp.getBytes()))
              .build();

      String splitExtendedProp = "split_extended_boostBuffer" + String.valueOf(i);

      PartitionProtobuf.NormalizedDatasetSplitInfo splitInfo =
          PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
              .setPartitionId(String.valueOf(i))
              .setExtendedProperty(ByteString.copyFrom(parquetXAttr.toByteArray()))
              .build();

      splitAndPartitionInfos.add(new SplitAndPartitionInfo(partitionInfo, splitInfo));
    }

    return splitAndPartitionInfos;
  }

  private OpProps getProps() {
    OpProps props = mock(OpProps.class);
    when(props.getOperatorId()).thenReturn(123);
    return props;
  }

  private TableFunctionConfig getTableFunctionConfig() {
    TableFunctionContext functionContext = mock(TableFunctionContext.class);
    BatchSchema fullSchema = new BatchSchema(Collections.EMPTY_LIST);
    when(functionContext.getFullSchema()).thenReturn(fullSchema);
    when(functionContext.getColumns()).thenReturn(Collections.EMPTY_LIST);
    when(functionContext.isArrowCachingEnabled()).thenReturn(true);

    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.DATA_FILE_SCAN, false, functionContext);
  }

  private String getPartitionExtendedProp(VarBinaryVector vector, int index)
      throws IOException, ClassNotFoundException {
    SplitAndPartitionInfo split = IcebergSerDe.deserializeFromByteArray(vector.get(index));
    return new String(split.getPartitionInfo().getExtendedProperty().toByteArray());
  }
}
