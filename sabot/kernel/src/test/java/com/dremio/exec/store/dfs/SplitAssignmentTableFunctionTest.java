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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.ExecTest;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

/**
 * Test for SplitAssignmentTableFunction
 */
public class SplitAssignmentTableFunctionTest extends ExecTest {

  VectorContainer incoming;
  VectorContainer outgoing;

  @Before
  public void setup() {
    incoming = new VectorContainer(allocator);
    outgoing = new VectorContainer(allocator); // closed by the table function
    incoming.addOrGet(Field.nullable(RecordReader.SPLIT_IDENTITY, CompleteType.VARBINARY.getType()));
    incoming.buildSchema();
  }

  @After
  public void tearDown() {
    incoming.close();
    incoming = null;
  }

  private List<SplitIdentity> getSplitIdentitiesWithBlockLocations(String path) {
    // 100 len blocks of the file located on two hosts 10.10.10.10/20 alternatively
    // splits also 100 sized

    List<SplitIdentity> splitIdentities = new ArrayList<>();

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

    for (int i = 0; i < 8; ++i) {
      splitIdentities.add(new SplitIdentity(path, blockLocationsList, i * 100, 100));
    }
    return splitIdentities;
  }

  @Test
  public void testSplitsAssignmentWithBlockLocations() throws Exception {
    SplitAssignmentTableFunction splitAssignmentTableFunction = getSplitAssignmentTableFunction();
    // create splitidentities
    List<SplitIdentity> splitIdentities = getSplitIdentitiesWithBlockLocations("/path/to/file");
    int numRecords = splitIdentities.size();
    VarBinaryVector splitVector = incoming.getValueAccessorById(VarBinaryVector.class, 0).getValueVector();
    for (int i = 0; i < numRecords; ++i) {
      splitVector.setSafe(i, IcebergSerDe.serializeToByteArray(splitIdentities.get(i)));
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

  private List<SplitIdentity> getSplitIdentitiesWithNoBlockLocations(String path) {
    return IntStream.range(0, 1024)
      .mapToObj(i -> new SplitIdentity(path, null, i * 100, 100))
      .collect(Collectors.toList());
  }

  @Test
  public void testSplitsAssignmentWithNoBlockLocations() throws Exception {
    SplitAssignmentTableFunction splitAssignmentTableFunction = getSplitAssignmentTableFunction();

    // create splitidentities
    List<SplitIdentity> splitIdentities = getSplitIdentitiesWithNoBlockLocations("/path/to/file");
    int numRecords = splitIdentities.size();
    VarBinaryVector splitVector = incoming.getValueAccessorById(VarBinaryVector.class, 0).getValueVector();
    for (int i = 0; i < numRecords; ++i) {
      splitVector.setSafe(i, IcebergSerDe.serializeToByteArray(splitIdentities.get(i)));
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

  private List<SplitIdentity> getSplitIdentitesWithPartLocalAndPartRemoteBlocks(String path) {
    List<SplitIdentity> splitIdentities = new ArrayList<>();

    // blocks distributed in 4 hosts (2 of them in target endpoints)
    // block size = 50 (split size is 100)
    // each block is duplicated in two hosts;
    String[] hosts = new String[]{"10.10.10.10", "10.10.10.20", "10.10.10.30", "10.10.10.40"};
    List<PartitionProtobuf.BlockLocations> blockLocations = IntStream.range(0, 1000)
      .mapToObj(i -> PartitionProtobuf.BlockLocations.newBuilder().addHosts(hosts[i%4]).addHosts(hosts[(i+1)%4]).setOffset(i * 50).setSize(50).build())
      .collect(Collectors.toList());

    PartitionProtobuf.BlockLocationsList blockLocationsList = PartitionProtobuf.BlockLocationsList.newBuilder().addAllBlockLocations(blockLocations).build();
    return IntStream.range(0, 500)
      .mapToObj(i -> new SplitIdentity(path, blockLocationsList, i * 100, 100))
      .collect(Collectors.toList());
  }

  @Test
  public void testSplitsAssignmentWithPartLocalAndPartRemoteAssignment() throws Exception {
    SplitAssignmentTableFunction splitAssignmentTableFunction = getSplitAssignmentTableFunction();

    // create splitidentities
    List<SplitIdentity> splitIdentities = getSplitIdentitesWithPartLocalAndPartRemoteBlocks("/path/to/file");
    int numRecords = splitIdentities.size();
    VarBinaryVector splitVector = incoming.getValueAccessorById(VarBinaryVector.class, 0).getValueVector();
    for (int i = 0; i < numRecords; ++i) {
      splitVector.setSafe(i, IcebergSerDe.serializeToByteArray(splitIdentities.get(i)));
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
    OperatorContext operatorContext = createOperatorContext();
    TableFunctionConfig tableFunctionConfig = createTableFunctionConfig();

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
    SplitAssignmentTableFunction splitAssignmentTableFunction = new SplitAssignmentTableFunction(operatorContext, tableFunctionConfig);
    splitAssignmentTableFunction.setup(incoming);
    return splitAssignmentTableFunction;
  }

  private OperatorContext createOperatorContext() {
    OperatorContext operatorContext = mock(OperatorContext.class);
    OptionManager optionManager = mock(OptionManager.class);
    when(operatorContext.getOptions()).thenReturn(optionManager);
    when(optionManager.getOption(ExecConstants.ASSIGNMENT_CREATOR_BALANCE_FACTOR)).thenReturn(1.5);
    when(operatorContext.createOutputVectorContainer()).thenReturn(outgoing);
    return operatorContext;
  }

  private TableFunctionConfig createTableFunctionConfig() {
    TableFunctionConfig tableFunctionConfig = mock(TableFunctionConfig.class);
    BatchSchema outputSchema = BatchSchema.newBuilder()
      .addField(Field.nullable(RecordReader.SPLIT_IDENTITY, CompleteType.VARBINARY.getType()))
      .addField(Field.nullable(HashPrelUtil.HASH_EXPR_NAME, CompleteType.INT.getType()))
      .build();
    when(tableFunctionConfig.getOutputSchema()).thenReturn(outputSchema);
    return tableFunctionConfig;
  }
}
