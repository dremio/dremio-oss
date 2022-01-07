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
package com.dremio.exec.store.dfs;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

/**
 * Test for SplitAssignmentTableFunction
 */
public class SplitAssignmentTableFunctionTest extends ExecTest {

  VectorContainer incoming;
  VectorContainer outgoing;
  ArrowBuf tmpBuf;

  @Before
  public void setup() {
    incoming = new VectorContainer(allocator);
    outgoing = new VectorContainer(allocator); // closed by the table function
    incoming.addOrGet(Field.nullable(RecordReader.SPLIT_IDENTITY, CompleteType.STRUCT.getType()));
    incoming.buildSchema();
    tmpBuf = allocator.buffer(4096);
  }

  @After
  public void tearDown() {
    incoming.close();
    tmpBuf.close();
  }

  @Test
  public void testSplitsAssignmentWithBlockLocations() throws Exception {
    SplitAssignmentTableFunction splitAssignmentTableFunction = spy(getSplitAssignmentTableFunction());
    // create splitidentities
    // 100 len blocks of the file located on two hosts 10.10.10.10/20 alternatively
    // splits also 100 sized

    PartitionProtobuf.BlockLocationsList blockLocationsList = PartitionProtobuf.BlockLocationsList.newBuilder()
      .addBlockLocations(PartitionProtobuf.BlockLocations.newBuilder().addHosts("10.10.10.10").setOffset(0).setSize(100).build())
      .addBlockLocations(PartitionProtobuf.BlockLocations.newBuilder().addHosts("10.10.10.20").setOffset(100).setSize(100).build())
      .addBlockLocations(PartitionProtobuf.BlockLocations.newBuilder().addHosts("10.10.10.10").setOffset(200).setSize(100).build())
      .addBlockLocations(PartitionProtobuf.BlockLocations.newBuilder().addHosts("10.10.10.20").setOffset(300).setSize(100).build())
      .addBlockLocations(PartitionProtobuf.BlockLocations.newBuilder().addHosts("10.10.10.10").setOffset(400).setSize(100).build())
      .addBlockLocations(PartitionProtobuf.BlockLocations.newBuilder().addHosts("10.10.10.20").setOffset(500).setSize(100).build())
      .addBlockLocations(PartitionProtobuf.BlockLocations.newBuilder().addHosts("10.10.10.10").setOffset(600).setSize(100).build())
      .addBlockLocations(PartitionProtobuf.BlockLocations.newBuilder().addHosts("10.10.10.20").setOffset(700).setSize(100).build())
      .build();

    doReturn(blockLocationsList).when(splitAssignmentTableFunction).getFileBlockLocations(anyString(), anyLong());

    List<SplitIdentity> splitIdentities = IntStream.range(0, 8).mapToObj(i -> new SplitIdentity("/path/to/file", i * 100, 100, 1000)).collect(Collectors.toList());
    int numRecords = splitIdentities.size();
    StructVector splitVector = incoming.getValueAccessorById(StructVector.class, 0).getValueVector();
    NullableStructWriter writer = splitVector.getWriter();
    for (int i = 0; i < numRecords; ++i) {
      IcebergUtils.writeSplitIdentity(writer, i, splitIdentities.get(i), tmpBuf);
    }
    incoming.setAllCount(numRecords);

    splitAssignmentTableFunction.startRow(0);
    splitAssignmentTableFunction.processRow(0, Integer.MAX_VALUE);

    // verify the split assignment
    IntVector hashVector = outgoing.getValueAccessorById(IntVector.class, 1).getValueVector();
    Map<Integer, Integer> fragmentSplitCount = new HashMap<>();
    for (int i = 0; i < numRecords; ++i) {
      fragmentSplitCount.merge(hashVector.get(i), 1, Integer::sum);
    }

    // all fragments are assigned
    Assert.assertEquals(4, fragmentSplitCount.size());
    // all are equally distributed
    for (int i = 1; i < fragmentSplitCount.size(); ++i) {
      Assert.assertEquals(fragmentSplitCount.get(i), fragmentSplitCount.get(i - 1));
    }

    splitAssignmentTableFunction.close();
  }

  @Test
  public void testSplitsAssignmentWithNoBlockLocations() throws Exception {
    SplitAssignmentTableFunction splitAssignmentTableFunction = spy(getSplitAssignmentTableFunction());

    doReturn(null).when(splitAssignmentTableFunction).getFileBlockLocations(anyString(), anyLong());

    // create splitidentities
    List<SplitIdentity> splitIdentities = IntStream.range(0, 1024)
      .mapToObj(i -> new SplitIdentity("path", i * 100, 100, 102400))
      .collect(Collectors.toList());
    int numRecords = splitIdentities.size();
    StructVector splitVector = incoming.getValueAccessorById(StructVector.class, 0).getValueVector();
    NullableStructWriter writer = splitVector.getWriter();
    for (int i = 0; i < numRecords; ++i) {
      IcebergUtils.writeSplitIdentity(writer, i, splitIdentities.get(i), tmpBuf);
    }
    incoming.setAllCount(numRecords);

    splitAssignmentTableFunction.startRow(0);
    splitAssignmentTableFunction.processRow(0, Integer.MAX_VALUE);

    // verify the split assignment
    IntVector hashVector = outgoing.getValueAccessorById(IntVector.class, 1).getValueVector();
    Map<Integer, Integer> fragmentSplitCount = new HashMap<>();
    for (int i = 0; i < numRecords; ++i) {
      fragmentSplitCount.merge(hashVector.get(i), 1, Integer::sum);
    }

    // all fragments are assigned
    Assert.assertEquals(4, fragmentSplitCount.size());
    int expectedSplitsPerMinorFrag = numRecords / fragmentSplitCount.size();
    for (int i = 0; i < fragmentSplitCount.size(); ++i) {
      // all are approximately equally distributed
      Assert.assertTrue(Math.abs(fragmentSplitCount.get(i) - expectedSplitsPerMinorFrag) < 0.05 * expectedSplitsPerMinorFrag);
    }

    splitAssignmentTableFunction.close();
  }

  @Test
  public void testSplitsAssignmentWithPartLocalAndPartRemoteAssignment() throws Exception {
    SplitAssignmentTableFunction splitAssignmentTableFunction = spy(getSplitAssignmentTableFunction());

    // blocks distributed in 4 hosts (2 of them in target endpoints)
    // block size = 50 (split size is 100)
    // each block is duplicated in two hosts;
    String[] hosts = new String[]{"10.10.10.10", "10.10.10.20", "10.10.10.30", "10.10.10.40"};
    List<PartitionProtobuf.BlockLocations> blockLocations = IntStream.range(0, 1000)
      .mapToObj(i -> PartitionProtobuf.BlockLocations.newBuilder().addHosts(hosts[i%4]).addHosts(hosts[(i+1)%4]).setOffset(i * 50).setSize(50).build())
      .collect(Collectors.toList());

    PartitionProtobuf.BlockLocationsList blockLocationsList = PartitionProtobuf.BlockLocationsList.newBuilder().addAllBlockLocations(blockLocations).build();

    doReturn(blockLocationsList).when(splitAssignmentTableFunction).getFileBlockLocations(anyString(), anyLong());

    // create splitidentities
    List<SplitIdentity> splitIdentities = IntStream.range(0, 500)
      .mapToObj(i -> new SplitIdentity("/path/to/file", i * 100, 100, 50000))
      .collect(Collectors.toList());
    int numRecords = splitIdentities.size();
    StructVector splitVector = incoming.getValueAccessorById(StructVector.class, 0).getValueVector();
    NullableStructWriter writer = splitVector.getWriter();
    for (int i = 0; i < numRecords; ++i) {
      IcebergUtils.writeSplitIdentity(writer, i, splitIdentities.get(i), tmpBuf);
    }
    incoming.setAllCount(numRecords);

    splitAssignmentTableFunction.startRow(0);
    splitAssignmentTableFunction.processRow(0, Integer.MAX_VALUE);

    // verify the split assignment
    IntVector hashVector = outgoing.getValueAccessorById(IntVector.class, 1).getValueVector();
    Map<Integer, Integer> fragmentSplitCount = new HashMap<>();
    for (int i = 0; i < numRecords; ++i) {
      fragmentSplitCount.merge(hashVector.get(i), 1, Integer::sum);
    }

    // all fragments are assigned
    Assert.assertEquals(4, fragmentSplitCount.size());
    // all are equally distributed
    int expectedSplitsPerMinorFrag = numRecords / fragmentSplitCount.size();
    for (int i = 0; i < fragmentSplitCount.size(); ++i) {
      Assert.assertEquals(fragmentSplitCount.get(i).intValue(), expectedSplitsPerMinorFrag);
    }

    splitAssignmentTableFunction.close();
  }

  private SplitAssignmentTableFunction getSplitAssignmentTableFunction() throws Exception {
    OperatorContext operatorContext = mock(OperatorContext.class);
    OptionManager optionManager = mock(OptionManager.class);
    when(operatorContext.getOptions()).thenReturn(optionManager);
    when(optionManager.getOption(ExecConstants.ASSIGNMENT_CREATOR_BALANCE_FACTOR)).thenReturn(1.5);
    when(operatorContext.createOutputVectorContainer()).thenReturn(outgoing);
    OperatorStats operatorStats = mock(OperatorStats.class);
    when(operatorContext.getStats()).thenReturn(operatorStats);
    TableFunctionConfig tableFunctionConfig = mock(TableFunctionConfig.class);
    BatchSchema outputSchema = BatchSchema.newBuilder()
      .addField(Field.nullable(RecordReader.SPLIT_IDENTITY, CompleteType.STRUCT.getType()))
      .addField(Field.nullable(HashPrelUtil.HASH_EXPR_NAME, CompleteType.INT.getType()))
      .build();
    when(tableFunctionConfig.getOutputSchema()).thenReturn(outputSchema);
    TableFunctionContext tableFunctionContext = mock(TableFunctionContext.class);
    when(tableFunctionConfig.getFunctionContext()).thenReturn(tableFunctionContext);
    StoragePluginId storagePluginId = mock(StoragePluginId.class);
    when(tableFunctionContext.getPluginId()).thenReturn(storagePluginId);
    FragmentExecutionContext fec = mock(FragmentExecutionContext.class);
    OpProps opProps = mock(OpProps.class);
    when(fec.getStoragePlugin(storagePluginId)).thenReturn(mock(FileSystemPlugin.class));

    // Two nodes with two minor fragment in each - total 4 minor fragments
    CoordinationProtos.NodeEndpoint endpoint1 = CoordinationProtos.NodeEndpoint.newBuilder().setAddress("10.10.10.10").setFabricPort(9000).build();
    CoordinationProtos.NodeEndpoint endpoint2 = CoordinationProtos.NodeEndpoint.newBuilder().setAddress("10.10.10.10").setFabricPort(9000).build();
    CoordinationProtos.NodeEndpoint endpoint3 = CoordinationProtos.NodeEndpoint.newBuilder().setAddress("10.10.10.20").setFabricPort(9000).build();
    CoordinationProtos.NodeEndpoint endpoint4 = CoordinationProtos.NodeEndpoint.newBuilder().setAddress("10.10.10.20").setFabricPort(9000).build();

    List<CoordinationProtos.NodeEndpoint> endpoints = Arrays.asList(endpoint1, endpoint2, endpoint3, endpoint4);
    List<MinorFragmentEndpoint> minorFragmentEndpoints = IntStream.range(0, endpoints.size())
      .mapToObj(i -> new MinorFragmentEndpoint(i, endpoints.get(i)))
      .collect(Collectors.toList());

    when(operatorContext.getMinorFragmentEndpoints()).thenReturn(minorFragmentEndpoints);

    // create SplitAssignmentTableFunction
    SplitAssignmentTableFunction splitAssignmentTableFunction = new SplitAssignmentTableFunction(fec, operatorContext, opProps, tableFunctionConfig);
    splitAssignmentTableFunction.setup(incoming);
    return splitAssignmentTableFunction;
  }
}
