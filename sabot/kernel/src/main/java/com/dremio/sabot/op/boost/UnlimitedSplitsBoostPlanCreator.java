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
package com.dremio.sabot.op.boost;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.Root;
import com.dremio.exec.physical.config.BoostPOP;
import com.dremio.exec.physical.config.BoostTableFunctionContext;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.parquet.BoostBufferManager;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Class responsible for creating plan for query which creates arrow cache in the background. The
 * plan consists of 3 operators: 1) EasyScanOperator consisting of arrow record reader which reads
 * an arrow file having splits in batches 2) BoostTableFunction which gets a batch of splits and
 * boosts them 3) Empty Screen operator which passes on nothing on the screen.
 */
public class UnlimitedSplitsBoostPlanCreator {

  private OperatorContext context;
  private OpProps props;
  private FragmentExecutionContext fec;
  private TableFunctionConfig functionConfig;
  private Path arrowFilePath;
  private List<SchemaPath> columnsToBoost;
  private String spillFileDir;

  public UnlimitedSplitsBoostPlanCreator(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig,
      Path arrowFilePath,
      List<SchemaPath> columnsToBoost,
      String spillFileDir) {
    this.context = context;
    this.props = props;
    this.fec = fec;
    this.functionConfig = functionConfig;
    this.arrowFilePath = arrowFilePath;
    this.columnsToBoost = columnsToBoost;
    this.spillFileDir = spillFileDir;
  }

  public Root createBoostPlan() {
    BoostPOP boostPOP = createBoostScan();
    TableFunctionPOP tableFunctionPOP = createTableFunction(boostPOP);

    Screen root =
        new Screen(
            OpProps.prototype(props.getOperatorId() + 2, 1_000_000, Long.MAX_VALUE),
            tableFunctionPOP,
            true); // screen operator should not send any messages to coord

    return root;
  }

  private BoostPOP createBoostScan() {
    List<Field> fieldList =
        Arrays.asList(
            CompleteType.VARBINARY.toField(BoostBufferManager.SPLITS_VECTOR),
            CompleteType.VARBINARY.toField(BoostBufferManager.COL_IDS));
    BatchSchema schema = new BatchSchema(fieldList);

    BoostPOP easySubScan =
        new BoostPOP(
            props,
            new FileConfig().setType(FileType.ARROW),
            createSplitAndPartitionInfo(),
            schema,
            ImmutableList.of(
                functionConfig.getFunctionContext().getReferencedTables().iterator().next()),
            functionConfig.getFunctionContext().getPluginId(),
            ImmutableList.of(
                SchemaPath.getSimplePath(BoostBufferManager.SPLITS_VECTOR),
                SchemaPath.getSimplePath(BoostBufferManager.COL_IDS)),
            null,
            null,
            true);
    return easySubScan;
  }

  private TableFunctionPOP createTableFunction(BoostPOP easySubScan) {
    FileConfig fileConfig = functionConfig.getFunctionContext().getFormatSettings();

    TableFunctionContext functionContext =
        new BoostTableFunctionContext(
            spillFileDir,
            fileConfig,
            functionConfig.getFunctionContext().getFullSchema(),
            null,
            ImmutableList.of(
                functionConfig.getFunctionContext().getReferencedTables().iterator().next()),
            null,
            functionConfig.getFunctionContext().getPluginId(),
            null,
            columnsToBoost,
            null,
            null,
            false,
            false,
            true,
            functionConfig.getFunctionContext().getUserDefinedSchemaSettings());

    TableFunctionConfig config =
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.BOOST_TABLE_FUNCTION, false, functionContext);
    TableFunctionPOP tableFunctionPOP =
        new TableFunctionPOP(
            props.cloneWithNewIdAndSchema(props.getOperatorId() + 1, BoostTableFunction.SCHEMA),
            easySubScan,
            config);
    return tableFunctionPOP;
  }

  private List<SplitAndPartitionInfo> createSplitAndPartitionInfo() {
    EasyProtobuf.EasyDatasetSplitXAttr easyDatasetSplitXAttr =
        EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
            .setPath(arrowFilePath.toString())
            .setStart(0)
            .build();

    PartitionProtobuf.NormalizedPartitionInfo partitionInfo =
        PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId("0").build();

    ByteString serializedSplitXAttr = easyDatasetSplitXAttr.toByteString();

    PartitionProtobuf.NormalizedDatasetSplitInfo datasetSplitInfo =
        PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
            .setPartitionId("0")
            .setExtendedProperty(serializedSplitXAttr)
            .build();

    return ImmutableList.of(new SplitAndPartitionInfo(partitionInfo, datasetSplitInfo));
  }
}
