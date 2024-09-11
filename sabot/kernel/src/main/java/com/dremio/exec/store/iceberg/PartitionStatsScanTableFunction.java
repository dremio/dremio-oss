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

import static com.dremio.exec.store.SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_COLS;
import static com.dremio.exec.store.SystemSchemas.METADATA_FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.SNAPSHOT_ID;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import com.dremio.common.expression.BasePath;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.CarryForwardAwareTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.google.common.collect.Streams;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionStatsFileLocations;
import org.apache.iceberg.PartitionStatsMetadataUtil;
import org.apache.iceberg.io.FileIO;

public class PartitionStatsScanTableFunction extends AbstractTableFunction {

  private final FragmentExecutionContext fragmentExecutionContext;
  private final OpProps props;
  private SupportsIcebergMutablePlugin icebergMutablePlugin;
  private VarCharVector metadataInVector;
  private BigIntVector snapshotIdInVector;
  private VarCharVector filePathOutVector;
  private VarCharVector fileTypeOutVector;
  private List<TransferPair> transfers;
  private int inputRow = 0;
  private int partitionFileIdx;
  private final List<byte[]> partitionStatsFiles = new ArrayList<>();
  private boolean transfersProcessed = false;
  private FileIO io = null;
  private String schemeVariate;
  private Configuration conf;
  private String fsScheme;

  public PartitionStatsScanTableFunction(
      FragmentExecutionContext fragmentExecutionContext,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    this.fragmentExecutionContext = fragmentExecutionContext;
    this.props = props;
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    VectorContainer outgoing = (VectorContainer) super.setup(incoming);
    metadataInVector = (VarCharVector) getVectorFromSchemaPath(incoming, METADATA_FILE_PATH);
    snapshotIdInVector = (BigIntVector) getVectorFromSchemaPath(incoming, SNAPSHOT_ID);
    filePathOutVector = (VarCharVector) getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_PATH);
    fileTypeOutVector = (VarCharVector) getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_TYPE);

    // create transfer pairs for any additional input columns
    transfers =
        Streams.stream(incoming)
            .filter(
                vw ->
                    !CARRY_FORWARD_FILE_PATH_TYPE_COLS.contains(vw.getValueVector().getName())
                        && outgoing
                                .getSchema()
                                .getFieldId(BasePath.getSimple(vw.getValueVector().getName()))
                            != null)
            .map(
                vw ->
                    vw.getValueVector()
                        .makeTransferPair(
                            getVectorFromSchemaPath(outgoing, vw.getValueVector().getName())))
            .collect(Collectors.toList());

    icebergMutablePlugin =
        fragmentExecutionContext.getStoragePlugin(
            functionConfig.getFunctionContext().getPluginId());

    schemeVariate =
        ((CarryForwardAwareTableFunctionContext) functionConfig.getFunctionContext())
            .getSchemeVariate();
    return outgoing;
  }

  @Override
  public void startBatch(int records) {
    outgoing.allocateNew();
  }

  @Override
  public void startRow(int row) throws Exception {
    inputRow = row;
    partitionFileIdx = 0;
    byte[] pathBytes = metadataInVector.get(row);
    String metadataLocation = new String(pathBytes, StandardCharsets.UTF_8);

    if (io == null) {
      FileSystem fs =
          icebergMutablePlugin.createFSWithAsyncOptions(
              metadataLocation, props.getUserName(), context);
      io =
          icebergMutablePlugin.createIcebergFileIO(
              fs, context, null, functionConfig.getFunctionContext().getPluginId().getName(), null);
      conf = icebergMutablePlugin.getFsConfCopy();
      fsScheme = fs.getScheme();
    }
    long snapshotId = snapshotIdInVector.get(row);
    String partitionStatsMetadataFileName = PartitionStatsMetadataUtil.toFilename(snapshotId);
    String partitionStatsMetadataLocation =
        IcebergUtils.resolvePath(metadataLocation, partitionStatsMetadataFileName);
    PartitionStatsFileLocations partitionStatsLocations =
        PartitionStatsMetadataUtil.readMetadata(io, partitionStatsMetadataLocation);

    if (partitionStatsLocations != null) {
      // Partition stats have metadata file and partition files.
      partitionStatsFiles.add(
          getIcebergPath(partitionStatsMetadataLocation).getBytes(StandardCharsets.UTF_8));
      partitionStatsFiles.addAll(
          partitionStatsLocations.all().values().stream()
              .map(s -> getIcebergPath(s).getBytes(StandardCharsets.UTF_8))
              .collect(Collectors.toList()));
    }
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    int outIdx = startOutIndex;
    int maxIdx = startOutIndex + maxRecords;
    if (!transfersProcessed && outIdx < maxIdx) {
      final int transferOutIdx = outIdx;
      transfers.forEach(t -> t.copyValueSafe(inputRow, transferOutIdx));
      outIdx++;
      transfersProcessed = true;
    }

    while (partitionFileIdx < partitionStatsFiles.size() && outIdx < maxIdx) {
      filePathOutVector.setSafe(outIdx, partitionStatsFiles.get(partitionFileIdx++));
      fileTypeOutVector.setSafe(
          outIdx, IcebergFileType.PARTITION_STATS.name().getBytes(StandardCharsets.UTF_8));
      outIdx++;
    }
    outgoing.setAllCount(outIdx);
    return outIdx - startOutIndex;
  }

  @Override
  public void closeRow() throws Exception {
    partitionStatsFiles.clear();
    transfersProcessed = false;
    partitionFileIdx = 0;
  }

  private String getIcebergPath(String path) {
    return IcebergUtils.getIcebergPathAndValidateScheme(path, conf, fsScheme, schemeVariate);
  }
}
