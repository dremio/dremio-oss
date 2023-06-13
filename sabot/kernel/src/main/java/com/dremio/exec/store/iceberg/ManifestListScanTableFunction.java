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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.planner.physical.TableFunctionUtil.getDataset;
import static com.dremio.exec.store.SystemSchemas.METADATA_FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.SNAPSHOT_ID;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.expressions.Expressions;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.MutatorSchemaChangeCallBack;
import com.dremio.sabot.op.scan.ScanOperator.ScanMutator;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;

/**
 * Table function for Iceberg manifest list file scan
 */
public class ManifestListScanTableFunction extends AbstractTableFunction {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ManifestListScanTableFunction.class);
  private final FragmentExecutionContext fragmentExecutionContext;
  private final OperatorStats operatorStats;
  private final OpProps props;

  private SupportsIcebergMutablePlugin icebergMutablePlugin;
  private List<String> tablePath;

  private ScanMutator mutator;
  private MutatorSchemaChangeCallBack callBack = new MutatorSchemaChangeCallBack();

  private IcebergManifestListRecordReader manifestListRecordReader;

  private VarCharVector inputMetadataLocation;
  private BigIntVector inputSnapshotId;

  private int inputIndex;

  public ManifestListScanTableFunction(
    FragmentExecutionContext fragmentExecutionContext,
    OperatorContext context,
    OpProps props,
    TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    this.fragmentExecutionContext = fragmentExecutionContext;
    this.props = props;
    this.operatorStats = context.getStats();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);

    icebergMutablePlugin = fragmentExecutionContext.getStoragePlugin(functionConfig.getFunctionContext().getPluginId());
    tablePath = getDataset(functionConfig);

    inputMetadataLocation = (VarCharVector) getVectorFromSchemaPath(incoming, METADATA_FILE_PATH);
    inputSnapshotId = (BigIntVector) getVectorFromSchemaPath(incoming, SNAPSHOT_ID);

    VectorContainer outgoing = (VectorContainer) super.setup(incoming);
    this.mutator = new ScanMutator(outgoing, context, callBack);
    this.mutator.allocate();

    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    inputIndex = row;

    // Initialize the reader for the current processing snapshot id
    byte[] pathBytes = inputMetadataLocation.get(inputIndex);
    String metadataLocation = new String(pathBytes, StandardCharsets.UTF_8);
    Long snapshotId = inputSnapshotId.get(inputIndex);
    TableFunctionContext functionContext = functionConfig.getFunctionContext();

    final IcebergExtendedProp icebergExtendedProp = new IcebergExtendedProp(
      null,
      IcebergSerDe.serializeToByteArray(Expressions.alwaysTrue()),
      snapshotId,
      null
    );

    manifestListRecordReader = new IcebergManifestListRecordReader(context, metadataLocation, icebergMutablePlugin,
      tablePath, functionContext.getPluginId().getName(), functionContext.getFullSchema(), props,
      functionContext.getPartitionColumns(), icebergExtendedProp, ManifestContentType.ALL);

    manifestListRecordReader.setup(mutator);
    operatorStats.addLongStat(TableFunctionOperator.Metric.NUM_SNAPSHOT_IDS, 1L);
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    int outputCount = manifestListRecordReader.nextBatch(startOutIndex, startOutIndex + maxRecords);
    int totalRecordCount = startOutIndex + outputCount;
    outgoing.forEach(vw -> vw.getValueVector().setValueCount(totalRecordCount));
    outgoing.setRecordCount(totalRecordCount);
    return outputCount;
  }

  @Override
  public void closeRow() throws Exception {
    manifestListRecordReader.close();
    manifestListRecordReader = null;
  }
}
