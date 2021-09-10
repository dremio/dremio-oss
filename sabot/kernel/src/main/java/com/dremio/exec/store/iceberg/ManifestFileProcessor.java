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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.io.CloseableIterator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.annotations.VisibleForTesting;

/**
 * Process ManifestFile. This class iterates over each datafile in manifest file and give to data processor one at a time
 */
public class ManifestFileProcessor implements AutoCloseable {
  private final FileSystem fs;
  private final List<String> dataset;
  private final OperatorContext context;
  private final String datasourcePluginUID;
  private final OperatorStats operatorStats;
  private final DatafileProcessor datafileProcessor;
  private final Configuration conf;

  private DataFile currentFile;
  private CloseableIterator<DataFile> iterator;
  private ManifestReader<DataFile> manifestReader;

  public ManifestFileProcessor(FragmentExecutionContext fec,
                               OperatorContext context, OpProps props,
                               TableFunctionConfig functionConfig) {
    FileSystemPlugin fileSystemPlugin = getStoragePlugin(fec, functionConfig.getFunctionContext());
    this.fs = createFs(context, props, fileSystemPlugin);
    this.conf = getConfiguration(fileSystemPlugin);
    this.context = context;
    this.operatorStats = context.getStats();
    this.dataset = getDataset(functionConfig);
    this.datasourcePluginUID = getDatasourcePluginId(functionConfig.getFunctionContext());
    this.datafileProcessor = new DatafileProcessorFactory(fec, props, context).getDatafileProcessor(functionConfig);
  }

  public void setup(VectorAccessible incoming, VectorContainer outgoing) {
    datafileProcessor.setup(incoming, outgoing);
  }

  public void setupManifestFile(ManifestFile manifestFile) {
    manifestReader = getManifestReader(manifestFile);
    iterator = manifestReader.iterator();
    datafileProcessor.initialise(manifestReader.spec());
  }

  public int process(int startOutIndex, int maxOutputCount) throws Exception {
    int currentOutputCount = 0;
    while ((currentFile != null || iterator.hasNext()) && currentOutputCount < maxOutputCount) {
      if (currentFile == null) {
        nextDataFile();
      }
      int outputRecords = datafileProcessor.processDatafile(currentFile,
        startOutIndex + currentOutputCount,
        maxOutputCount - currentOutputCount);
      if (outputRecords == 0) {
        resetCurrentDataFile();
      } else {
        currentOutputCount += outputRecords;
      }
    }
    return currentOutputCount;
  }

  public void closeManifestFile() throws Exception {
    AutoCloseables.close(iterator, manifestReader);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(datafileProcessor);
  }

  @VisibleForTesting
  ManifestReader<DataFile> getManifestReader(ManifestFile manifestFile) {
    return ManifestFiles.read(manifestFile, new DremioFileIO(fs, context, dataset, datasourcePluginUID, manifestFile.length(), conf));
  }

  private void nextDataFile() {
    currentFile = iterator.next();
    operatorStats.addLongStat(TableFunctionOperator.Metric.NUM_DATA_FILE, 1);
  }

  private void resetCurrentDataFile() {
    currentFile = null;
    datafileProcessor.closeDatafile();
  }

  private static StoragePluginId getPluginId(TableFunctionContext functionContext) {
    if (functionContext.getInternalTablePluginId() != null) {
      // This happens when an internal Iceberg table is created by Dremio, and we want to
      // use this plugin for accessing Manifest files.
      return functionContext.getInternalTablePluginId();
    } else {
      return functionContext.getPluginId();
    }
  }

  private static String getDatasourcePluginId(TableFunctionContext functionContext) {
    return functionContext.getPluginId().getName();
  }

  private static FileSystem createFs(OperatorContext context, OpProps props, FileSystemPlugin fileSystemPlugin) {
    try {
      return fileSystemPlugin.createFS(props.getUserName(), context);
    } catch (IOException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  private static List<String> getDataset(TableFunctionConfig functionConfig) {
    TableFunctionContext functionContext = functionConfig.getFunctionContext();
    Collection<List<String>> referencedTables = functionContext.getReferencedTables();
    return referencedTables != null ? referencedTables.iterator().next() : null;
  }

  private static Configuration getConfiguration(FileSystemPlugin fileSystemPlugin) {
    return fileSystemPlugin.getFsConfCopy();
  }

  private FileSystemPlugin getStoragePlugin(FragmentExecutionContext fec, TableFunctionContext functionContext) {
    StoragePluginId pluginId = getPluginId(functionContext);
    try {
      return fec.getStoragePlugin(pluginId);
    } catch (ExecutionSetupException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

}
