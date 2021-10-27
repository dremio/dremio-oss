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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.config.DremioConfig;
import com.dremio.exec.hadoop.HadoopFileSystem;
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

/**
 * Tests for {@link BoostBufferManager}
 */

public class TestBoostBufferManager {
  private BufferAllocator testAllocator;
  private SpillService spillService;

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  public TemporaryFolder spillParentDir = new TemporaryFolder();


  public class NoopScheduler implements SchedulerService {
    private Runnable taskToRun;

    @Override
    public Cancellable schedule(Schedule schedule, Runnable task) {
      taskToRun = task;
      return new Cancellable() {
        @Override
        public void cancel(boolean mayInterruptIfRunning) { }

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
    public void start() {
    }

    @Override
    public void close() {
    }
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
    when(config.getStringList(DremioConfig.SPILLING_PATH_STRING)).thenReturn(ImmutableList.of(spillDir.getPath()));
    final NoopScheduler schedulerService = new NoopScheduler();
    spillService = new SpillServiceImpl(config, new TestSpillServiceOptions(),
      new Provider<SchedulerService>() {
        @Override
        public SchedulerService get() {
          return schedulerService;
        }
      }
    );
    spillService.start();
  }


  private OperatorContext getCtx() {
    OperatorContext operatorContext = mock(OperatorContext.class, RETURNS_DEEP_STUBS);
    when(operatorContext.getAllocator()).thenReturn(testAllocator);
    ExecProtos.FragmentHandle fragmentHandle =  ExecProtos.FragmentHandle.getDefaultInstance();
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
    BoostBufferManager boostBufferManager = new BoostBufferManager(getCtx());
    List<SplitAndPartitionInfo> splitAndPartitionInfos = buildSplit(7);

    List<SchemaPath> columns = ImmutableList.of(SchemaPath.getSimplePath("column1"));
    splitAndPartitionInfos.stream().forEach(x -> {
      try {
        boostBufferManager.addSplit(x, columns);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });

    boostBufferManager.close();

    Path arrowFilePath = boostBufferManager.getArrowFilePath();
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());


    List<Field> fieldList = Arrays.asList(
      CompleteType.VARBINARY.toField("splitInfo")
    );

    BatchSchema schema = new BatchSchema(fieldList);

    try(VectorContainer container = VectorContainer.create(testAllocator, schema);
        ArrowRecordReader reader = new ArrowRecordReader(getCtx(),
          fs, arrowFilePath, ImmutableList.of(SchemaPath.getSimplePath("splitInfo")));
        SampleMutator mutator = new SampleMutator(testAllocator))
    {
      Field splitInfo = CompleteType.VARBINARY.toField("splitInfo");
      mutator.addField(splitInfo, VarBinaryVector.class);
      mutator.getContainer().buildSchema();
      reader.allocate(mutator.getFieldVectorMap());
      reader.setup(mutator);

      int totalRecordCount = 0;
      int currentCount = 0;

      //read every thing using arrow record reader. Should read 7 splits.
      while((currentCount = reader.next()) != 0) {
        totalRecordCount += currentCount;
      }

      assertEquals(totalRecordCount, 7);
      //Only one batch is read so just assert the vectors
      assertEquals("partition_extended_boostBuffer2", getPartitionExtendedProp((VarBinaryVector) mutator.getVector("splitInfo"),2));
      assertEquals("partition_extended_boostBuffer5", getPartitionExtendedProp((VarBinaryVector) mutator.getVector("splitInfo"),5));
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testBoostBufferManagerWithMultipleBatches() throws Exception {
    BoostBufferManager boostBufferManager = new BoostBufferManager(getCtx());

    //We will write 54 splits with batch size of 10. So we will write 6 batches.
    List<SplitAndPartitionInfo> splitAndPartitionInfos = buildSplit(54);

    List<SchemaPath> columns = ImmutableList.of(SchemaPath.getSimplePath("column1"));
    boostBufferManager.setBatchSize(10);

    splitAndPartitionInfos.stream().forEach(x -> {
      try {
        boostBufferManager.addSplit(x, columns);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });


    boostBufferManager.close();
    Path arrowFilePath = boostBufferManager.getArrowFilePath();
    FileSystem fs = HadoopFileSystem.getLocal(new Configuration());


    List<Field> fieldList = Arrays.asList(
      CompleteType.VARBINARY.toField("splitInfo")
    );

    BatchSchema schema = new BatchSchema(fieldList);

    try(VectorContainer container = VectorContainer.create(testAllocator, schema);
        ArrowRecordReader reader = new ArrowRecordReader(getCtx(), fs, arrowFilePath, ImmutableList.of(SchemaPath.getSimplePath("splitInfo")));
        SampleMutator mutator = new SampleMutator(testAllocator))
    {
      Field splitInfo = CompleteType.VARBINARY.toField("splitInfo");
      mutator.addField(splitInfo, VarBinaryVector.class);
      mutator.getContainer().buildSchema();
      reader.allocate(mutator.getFieldVectorMap());
      reader.setup(mutator);

      int totalRecordCount = 0;
      int currentCount = 0;
      int batchesRead = 0;
      //read every thing using arrow record reader. Should read 54 splits.
      while((currentCount = reader.next()) != 0) {
        totalRecordCount += currentCount;
        //assert the value of first element in each batch read.
        //Batch size will be ten and we would read 6 batches
        assertEquals("partition_extended_boostBuffer" + batchesRead * 10, getPartitionExtendedProp((VarBinaryVector) mutator.getVector("splitInfo"),0));
        batchesRead++;
      }
      assertEquals(totalRecordCount, 54);
      assertEquals(batchesRead, 6);
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private List<SplitAndPartitionInfo> buildSplit(int num) {
    List<SplitAndPartitionInfo> splitAndPartitionInfos = new ArrayList<>();

    for (int i = 0; i < num; ++i) {
      String extendedProp = "partition_extended_boostBuffer" + String.valueOf(i);
      PartitionProtobuf.NormalizedPartitionInfo partitionInfo = PartitionProtobuf.NormalizedPartitionInfo
        .newBuilder()
        .setId(String.valueOf(i))
        .setSize(i * 1000)
        .setExtendedProperty(ByteString.copyFrom(extendedProp.getBytes()))
        .build();

        String splitExtendedProp = "split_extended_boostBuffer" + String.valueOf(i);

        PartitionProtobuf.NormalizedDatasetSplitInfo splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
          .newBuilder()
          .setPartitionId(String.valueOf(i))
          .setExtendedProperty(ByteString.copyFrom(splitExtendedProp.getBytes()))
          .build();

        splitAndPartitionInfos.add(new SplitAndPartitionInfo(partitionInfo, splitInfo));
    }

    return splitAndPartitionInfos;
  }

  private String getPartitionExtendedProp(VarBinaryVector vector, int index) throws IOException, ClassNotFoundException {
    SplitAndPartitionInfo split = IcebergSerDe.deserializeFromByteArray(vector.get(index));
    return new String(split.getPartitionInfo().getExtendedProperty().toByteArray());
  }

}
