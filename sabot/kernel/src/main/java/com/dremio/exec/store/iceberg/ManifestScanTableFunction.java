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

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.io.IOException;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.annotations.VisibleForTesting;

/**
 * Table function for Iceberg manifest file scan
 */
public class ManifestScanTableFunction extends AbstractTableFunction {
  private final OperatorStats operatorStats;
  private final ManifestFileProcessor manifestFileProcessor;
  private final ManifestContent manifestContent;

  private VarBinaryVector inputManifestFiles;

  public ManifestScanTableFunction(OperatorContext context, TableFunctionConfig functionConfig,
                                   ManifestFileProcessor manifestFileProcessor) {
    super(context, functionConfig);
    this.operatorStats = context.getStats();
    this.manifestFileProcessor = manifestFileProcessor;
    this.manifestContent = functionConfig.getFunctionContext(ManifestScanTableFunctionContext.class)
        .getManifestContent();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);
    inputManifestFiles = (VarBinaryVector) getVectorFromSchemaPath(incoming, RecordReader.SPLIT_INFORMATION);
    manifestFileProcessor.setup(incoming, outgoing);
    return outgoing;
  }

  @Override
  public void startBatch(int records) {
    outgoing.allocateNew();
  }

  @Override
  public void startRow(int row) throws Exception {
    ManifestFile manifestFile = getManifestFile(row);
    manifestFileProcessor.setupManifestFile(manifestFile);
  }

  @Override
  public int processRow(int startOutIndex, int maxOutputCount) throws Exception {
    int outputCount = manifestFileProcessor.process(startOutIndex, maxOutputCount);
    int totalRecordCount = startOutIndex + outputCount;
    outgoing.forEach(vw -> vw.getValueVector().setValueCount(totalRecordCount));
    outgoing.setRecordCount(totalRecordCount);
    return outputCount;
  }

  @Override
  public void closeRow() throws Exception {
    operatorStats.setReadIOStats();
    manifestFileProcessor.closeManifestFile();
  }

  @Override
  public void close() throws Exception {
    manifestFileProcessor.closeManifestFile();
    AutoCloseables.close(manifestFileProcessor, super::close);
  }

  @VisibleForTesting
  ManifestFile getManifestFile(int manifestFileIndex) throws IOException, ClassNotFoundException {
    ManifestFile manifestFile = IcebergSerDe.deserializeFromByteArray(inputManifestFiles.get(manifestFileIndex));
    operatorStats.addLongStat(manifestContent == ManifestContent.DATA ?
        TableFunctionOperator.Metric.NUM_MANIFEST_FILE : TableFunctionOperator.Metric.NUM_DELETE_MANIFESTS, 1);
    return manifestFile;
  }
}
