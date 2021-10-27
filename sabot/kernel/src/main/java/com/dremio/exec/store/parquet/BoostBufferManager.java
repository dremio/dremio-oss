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
import java.util.List;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.easy.arrow.ArrowFormatPluginConfig;
import com.dremio.exec.store.easy.arrow.ArrowRecordWriter;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.spill.SpillDirectory;
import com.dremio.service.spill.SpillService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

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

  private int batchSize = 4_000;
  private int currentPosition = 0;
  private List<SchemaPath> columnsToBoost;
  private OperatorContext context;

  private VectorContainer container;
  private VarBinaryVector splitsToBoost;
  private SpillService diskManager;
  private String id;
  private ArrowRecordWriter writer;
  private Path arrowFilePath;

  public BoostBufferManager(OperatorContext context) throws IOException {
    this.context = context;

    final String qid = QueryIdHelper.getQueryId(context.getFragmentHandle().getQueryId());
    final int majorFragmentId = context.getFragmentHandle().getMajorFragmentId();
    final int minorFragmentId = context.getFragmentHandle().getMinorFragmentId();
    id = String.format("boostSplits-%s.%s.%s",
      qid, majorFragmentId, minorFragmentId);

    this.diskManager = context.getSpillService();
    setupSplitsVectorContainer();
  }


  public void addSplit(SplitAndPartitionInfo inputSplit, List<SchemaPath> columnsToBoost) throws IOException {
    Preconditions.checkArgument(currentPosition < batchSize);
    splitsToBoost.setSafe(currentPosition, IcebergSerDe.serializeToByteArray(inputSplit));
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
  }

  public void setupSplitsVectorContainer() throws IOException {
    diskManager.makeSpillSubdirs(id);
    SpillDirectory spillDirectory = diskManager.getSpillSubdir(id);

    container = new VectorContainer();
    ScanOperator.ScanMutator mutator = new ScanOperator.ScanMutator(container, Maps.newHashMap(), context, null);

    Field splitInfo = CompleteType.VARBINARY.toField("splitInfo");
    mutator.addField(splitInfo, VarBinaryVector.class);

    mutator.getContainer().buildSchema();

    splitsToBoost = (VarBinaryVector) mutator.getVector("splitInfo");
    splitsToBoost.setInitialCapacity(batchSize);
    arrowFilePath = Path.of((new org.apache.hadoop.fs.Path(spillDirectory.getSpillDirPath(), "splitSpill.dremarrow1")).toUri());

    writer = new ArrowRecordWriter(
      context,
      arrowFilePath.toString(),
      new ArrowFormatPluginConfig(), HadoopFileSystem.get(spillDirectory.getFileSystem()));

    RecordWriter.WriteStatsListener byteCountListener = (b) -> {};
    RecordWriter.OutputEntryListener fileWriteListener = (a, b, c, d, e, f, g, partition) -> {};

    writer.setup(container, fileWriteListener, byteCountListener);
  }

  public Path getArrowFilePath() {
    return arrowFilePath;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public void close() throws Exception {
    if(currentPosition > 0) {
      addToArrowFile();
    }
    container.close();
    writer.close();
  }
}
