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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.serde.ProtobufByteStringSerDe;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.Root;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.easy.arrow.ArrowFormatPluginConfig;
import com.dremio.exec.store.easy.arrow.ArrowRecordWriter;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.sabot.op.boost.UnlimitedSplitsBoostPlanCreator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedDatasetSplitInfo;
import com.dremio.service.spill.SpillDirectory;
import com.dremio.service.spill.SpillService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

/**
 * Class responsible for handling arrow cache creation with unlimited splits. One instance is created
 * per minor fragment of ParquetScanTableFunction
 * Has 3 responsibilities
 *
 * 1) Has a Varbinary vector of size 4k which has serialised splits into it. Whenever vector size reaches 4k
 * the splits are flushed onto the the disk into an running arrow file. Location of file is one of the spill directories
 * taken from spill service into a subDir with name boostSplits-queryId-minorFragId-majorFragId.
 *
 * 2) On close it creates a plan to fire a query on local machine which should read the arrowFile with splits
 * in the background and then boost those splits.
 */
public class BoostBufferManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BoostBufferManager.class);

  public static final String SPLITS_VECTOR = RecordReader.SPLIT_INFORMATION;
  public static final String COL_IDS = RecordReader.COL_IDS;

  private int batchSize = 4_000;
  private int currentPosition = 0;
  private List<SchemaPath> columnsToBoost;
  private OperatorContext context;

  private VectorContainer container;
  private VarBinaryVector splitsToBoost;
  private VarBinaryVector colIdVector;
  private SpillService diskManager;
  private String id;
  private ArrowRecordWriter writer;
  private Path arrowFilePath;
  private OpProps props;
  private FragmentExecutionContext fec;
  private TableFunctionConfig functionConfig;
  private int batchesProcessed = 0;
  private boolean isArrowCachingEnabled;

  public BoostBufferManager(FragmentExecutionContext fec,
                            OperatorContext context,
                            OpProps props,
                            TableFunctionConfig functionConfig) throws IOException {
    this.context = context;
    this.props = props;
    this.fec = fec;
    this.functionConfig = functionConfig;
    this.isArrowCachingEnabled = functionConfig.getFunctionContext().isArrowCachingEnabled() ||
      context.getOptions().getOption(ExecConstants.ENABLE_PARQUET_ARROW_CACHING);

    if(!isArrowCachingEnabled) {
      return;
    }
    final String qid = QueryIdHelper.getQueryId(context.getFragmentHandle().getQueryId());
    final int majorFragmentId = context.getFragmentHandle().getMajorFragmentId();
    final int minorFragmentId = context.getFragmentHandle().getMinorFragmentId();
    final int operatorId = props.getLocalOperatorId();
    id = String.format("boostSplits-%s.%s.%s.%s",
      qid, majorFragmentId, minorFragmentId, operatorId);

    this.diskManager = context.getSpillService();
    setupSplitsVectorContainer();
  }


  public void addSplit(SplitAndPartitionInfo inputSplit, ParquetProtobuf.ParquetDatasetSplitScanXAttr splitScanXAttr, List<SchemaPath> columnsToBoost) throws IOException {
    if(!isArrowCachingEnabled) {
      return;
    }
    Preconditions.checkArgument(currentPosition < batchSize);
    NormalizedDatasetSplitInfo normalizedDatasetSplitInfo = NormalizedDatasetSplitInfo.newBuilder()
      .setPartitionId(inputSplit.getDatasetSplitInfo().getPartitionId())
      .setExtendedProperty(splitScanXAttr.toByteString())
      .build();

    splitsToBoost.setSafe(currentPosition, IcebergSerDe.serializeToByteArray(
    new SplitAndPartitionInfo(inputSplit.getPartitionInfo(), normalizedDatasetSplitInfo)));
    this.columnsToBoost = columnsToBoost;
    currentPosition++;
    if(currentPosition == batchSize) {
      //We filled out the batch so boost the batch
      addToArrowFile();
      currentPosition = 0;
    }
  }

  public List<SchemaPath> getColumnsToBoost() {
    return columnsToBoost;
  }

  public void addToArrowFile() throws IOException {
    container.setRecordCount(currentPosition);
    container.setAllCount(currentPosition);
    writer.writeBatch(0, currentPosition);
    splitsToBoost.clear();
    splitsToBoost.setInitialCapacity(batchSize);
    batchesProcessed++;
  }

  public void setupSplitsVectorContainer() throws IOException {
    diskManager.makeSpillSubdirs(id);
    SpillDirectory spillDirectory = diskManager.getSpillSubdir(id);

    container = new VectorContainer();
    ScanOperator.ScanMutator mutator = new ScanOperator.ScanMutator(container, Maps.newHashMap(), context, null);

    Field splitInfo = CompleteType.VARBINARY.toField(SPLITS_VECTOR);
    Field colIds = CompleteType.VARBINARY.toField(COL_IDS);

    mutator.addField(splitInfo, VarBinaryVector.class);
    mutator.addField(colIds, VarBinaryVector.class);

    mutator.getContainer().buildSchema();

    splitsToBoost = (VarBinaryVector) mutator.getVector(SPLITS_VECTOR);
    splitsToBoost.setInitialCapacity(batchSize);

    colIdVector = (VarBinaryVector) mutator.getVector(COL_IDS);
    arrowFilePath = Path.of((new org.apache.hadoop.fs.Path(spillDirectory.getSpillDirPath(), "splitSpill.dremarrow1")).toUri());

    writer = new ArrowRecordWriter(
      context,
      arrowFilePath.toString(),
      new ArrowFormatPluginConfig(), HadoopFileSystem.get(spillDirectory.getFileSystem()));

    RecordWriter.WriteStatsListener byteCountListener = (b) -> {};
    RecordWriter.OutputEntryListener fileWriteListener = (a, b, c, d, e, f, g, partition, h) -> {};

    writer.setup(container, fileWriteListener, byteCountListener);
  }

  public Path getArrowFilePath() {
    return arrowFilePath;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public void setIcebergColumnIds(byte[] columnIds) {
    if(!isArrowCachingEnabled) {
      return;
    }

    if(colIdVector.getValueCount() == 0) {
      colIdVector.setSafe(0, columnIds);
    }
  }

  public void close() throws Exception {
    if(!isArrowCachingEnabled) {
      return;
    }
    if(currentPosition > 0) {
      addToArrowFile();
    }
    container.close();
    if(batchesProcessed > 0) {
      writer.close();
      createAndFireBoostQuery();
    } else {
      writer.abort();
      diskManager.deleteSpillSubdirs(id);
    }
  }

  void createAndFireBoostQuery() throws ForemanSetupException {
    UnlimitedSplitsBoostPlanCreator planCreator = new UnlimitedSplitsBoostPlanCreator(fec, context,
      props, functionConfig, arrowFilePath, columnsToBoost, id);

    Root rootPrel = planCreator.createBoostPlan();
    UserBitShared.QueryId queryId = context.getQueryIdForLocalQuery();
    int minorFragmentId = 0;
    int majorFragmentId = 0;
    ExecProtos.FragmentHandle handle =
      ExecProtos.FragmentHandle
        .newBuilder()
        .setMajorFragmentId(majorFragmentId)
        .setMinorFragmentId(minorFragmentId)
        .setQueryId(queryId)
        .build();

    // get plan as JSON
    ByteString plan;
    ByteString optionsData;
    try {
      plan = ProtobufByteStringSerDe.writeValue(context.getLpPersistence().getMapper(), rootPrel, ProtobufByteStringSerDe.Codec.NONE);
      optionsData = ProtobufByteStringSerDe.writeValue(context.getLpPersistence().getMapper(), context.getOptions().getNonDefaultOptions(), ProtobufByteStringSerDe.Codec.NONE);
    } catch (JsonProcessingException e) {
      throw new ForemanSetupException("Failure while trying to convert fragment into json.", e);
    }

    CoordExecRPC.PlanFragmentMajor major =
      CoordExecRPC.PlanFragmentMajor.newBuilder()
        .setForeman(fec.getForemanEndpoint()) // get foreman from Scan
        .setFragmentJson(plan)
        .setHandle(handle.toBuilder().clearMinorFragmentId().build())
        .setLeafFragment(true)
        .setContext(fec.getQueryContextInformation())
        .setMemInitial(rootPrel.getProps().getMemReserve() + 2 * props.getMemReserve())
        .setOptionsJson(optionsData)
        .setCredentials(UserBitShared.UserCredentials
          .newBuilder()
          .setUserName(props.getUserName())
          .build())
        .setPriority(CoordExecRPC.FragmentPriority.newBuilder().setWorkloadClass(UserBitShared.WorkloadClass.BACKGROUND).build())
        .setFragmentCodec(CoordExecRPC.FragmentCodec.NONE)
        .addAllAllAssignment(Collections.emptyList())
        .build();

    // minor with empty assignment, collector, attrs
    CoordExecRPC.PlanFragmentMinor minor = CoordExecRPC.PlanFragmentMinor.newBuilder()
      .setMajorFragmentId(majorFragmentId)
      .setMinorFragmentId(minorFragmentId)
      .setAssignment(CoordinationProtos.NodeEndpoint.newBuilder().build())
      .setMemMax(fec.getQueryContextInformation().getQueryMaxAllocation())
      .addAllCollector(Collections.emptyList())
      .addAllAttrs(Collections.emptyList())
      .build();

    logger.debug("Starting boost fragment with queryID: {} to boost columns {} of table [{}]",
      QueryIdHelper.getQueryId(queryId), columnsToBoost,
      functionConfig.getFunctionContext().getReferencedTables().stream().flatMap(Collection::stream).collect(Collectors.joining(".")));

    context.startFragmentOnLocal(new PlanFragmentFull(major, minor));
  }

}
