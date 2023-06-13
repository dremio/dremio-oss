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
import static com.dremio.exec.store.iceberg.IcebergUtils.getPartitionStatsFiles;
import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.iceberg.PartitionStatsFileLocations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

import com.dremio.common.expression.BasePath;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.google.common.collect.Streams;

public class PartitionStatsScanTableFunction extends AbstractTableFunction {

  private final FragmentExecutionContext fragmentExecutionContext;
  private final OperatorStats operatorStats;
  private final OpProps props;
  private SupportsIcebergMutablePlugin icebergMutablePlugin;
  private VarCharVector metadataInVector;
  private BigIntVector snapshotIdInVector;
  private VarCharVector filePathOutVector;
  private VarCharVector fileTypeOutVector;
  private List<TransferPair> transfers;
  private int inputRow = 0;
  private int partitionFileIdx;
  private List<byte[]> partitionStatsFiles = new ArrayList<>();
  private boolean transfersProcessed = false;
  private TableMetadata tableMetadata = null;
  private FileIO io = null;

  public PartitionStatsScanTableFunction(
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
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    VectorContainer outgoing = (VectorContainer) super.setup(incoming);
    metadataInVector = (VarCharVector) getVectorFromSchemaPath(incoming, METADATA_FILE_PATH);
    snapshotIdInVector = (BigIntVector) getVectorFromSchemaPath(incoming, SNAPSHOT_ID);
    filePathOutVector = (VarCharVector) getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_PATH);
    fileTypeOutVector = (VarCharVector) getVectorFromSchemaPath(outgoing, SystemSchemas.FILE_TYPE);

    // create transfer pairs for any additional input columns
    transfers = Streams.stream(incoming)
      .filter(vw -> !CARRY_FORWARD_FILE_PATH_TYPE_COLS.contains(vw.getValueVector().getName()) &&
        outgoing.getSchema().getFieldId(BasePath.getSimple(vw.getValueVector().getName())) != null)
      .map(vw -> vw.getValueVector().makeTransferPair(
        getVectorFromSchemaPath(outgoing, vw.getValueVector().getName())))
      .collect(Collectors.toList());

    icebergMutablePlugin = fragmentExecutionContext.getStoragePlugin(functionConfig.getFunctionContext().getPluginId());

    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    inputRow = row;
    partitionFileIdx = 0;

    if (tableMetadata == null || io == null) {
      byte[] pathBytes = metadataInVector.get(row);
      String metadataLocation = new String(pathBytes, StandardCharsets.UTF_8);
      FileSystem fs = icebergMutablePlugin.createFSWithAsyncOptions(metadataLocation, props.getUserName(), context);
      io = icebergMutablePlugin.createIcebergFileIO(fs, context, null, functionConfig.getFunctionContext().getPluginId().getName(), null);
      tableMetadata = TableMetadataParser.read(io, metadataLocation);
    }
    long snapshotId = snapshotIdInVector.get(row);
    Snapshot snapshot = tableMetadata.snapshot(snapshotId);

    if (snapshot.partitionStatsMetadata() != null) {
      String partitionStatsMetadataLocation = snapshot.partitionStatsMetadata().metadataFileLocation();
      PartitionStatsFileLocations partitionStatsLocations = getPartitionStatsFiles(io, partitionStatsMetadataLocation);
      if (partitionStatsLocations != null) {
        // Partition stats have metadata file and partition files.
        partitionStatsFiles.add(partitionStatsMetadataLocation.getBytes(StandardCharsets.UTF_8));
        partitionStatsFiles.addAll(partitionStatsLocations.all().entrySet().stream()
          .map(e -> e.getValue().getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList()));
      }
    }
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    int outIdx = startOutIndex;
    int maxIdx = startOutIndex + maxRecords;
    if (!transfersProcessed) {
      transfers.forEach(t -> t.copyValueSafe(inputRow, startOutIndex));
      outIdx++;
      transfersProcessed = true;
    }

    while (partitionFileIdx < partitionStatsFiles.size() && outIdx < maxIdx) {
      filePathOutVector.setSafe(outIdx, partitionStatsFiles.get(partitionFileIdx++));
      fileTypeOutVector.setSafe(outIdx, IcebergFileType.PARTITION_STATS.name().getBytes(StandardCharsets.UTF_8));
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
}
