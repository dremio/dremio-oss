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

import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DremioManifestReaderUtils;
import org.apache.iceberg.DremioManifestReaderUtils.ManifestEntryWrapper;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FilterIterator;

/**
 * Process ManifestFile. This class iterates over each datafile in manifest file and give to data
 * processor one at a time
 */
public class ManifestFileProcessor implements AutoCloseable {
  private final OpProps opProps;
  private final SupportsIcebergRootPointer icebergRootPointerPlugin;
  private final List<String> dataset;
  private final OperatorContext context;
  private final String datasourcePluginUID;
  private final OperatorStats operatorStats;
  private final ManifestEntryProcessor manifestEntryProcessor;
  private final Configuration conf;
  private ManifestEntryWrapper<?> currentManifestEntry;
  private CloseableIterator<? extends ManifestEntryWrapper<?>> iterator;
  private ManifestReader<?> manifestReader;
  private ManifestScanFilters manifestScanFilters;
  private Map<Integer, PartitionSpec> partitionSpecMap;

  public ManifestFileProcessor(
      FragmentExecutionContext fec,
      OperatorContext context,
      OpProps props,
      TableFunctionConfig functionConfig) {
    this.icebergRootPointerPlugin = getStoragePlugin(fec, functionConfig.getFunctionContext());
    this.opProps = props;
    this.conf = getConfiguration(icebergRootPointerPlugin);
    Preconditions.checkState(context != null, "Unexpected state");
    this.context = context;
    this.operatorStats = context.getStats();
    this.dataset = getDataset(functionConfig);
    this.datasourcePluginUID = getDatasourcePluginId(functionConfig.getFunctionContext());
    this.manifestEntryProcessor =
        new ManifestEntryProcessorFactory(fec, props, context)
            .getManifestEntryProcessor(functionConfig);
    ManifestScanTableFunctionContext functionContext =
        functionConfig.getFunctionContext(ManifestScanTableFunctionContext.class);
    if (functionContext.getJsonPartitionSpecMap() != null) {
      partitionSpecMap =
          IcebergSerDe.deserializeJsonPartitionSpecMap(
              deserializedJsonAsSchema(functionContext.getIcebergSchema()),
              functionContext.getJsonPartitionSpecMap().toByteArray());
    } else if (functionContext.getPartitionSpecMap() != null) {
      partitionSpecMap =
          IcebergSerDe.deserializePartitionSpecMap(
              functionContext.getPartitionSpecMap().toByteArray());
    }
    this.manifestScanFilters =
        ((ManifestScanTableFunctionContext) functionConfig.getFunctionContext())
            .getManifestScanFilters();
  }

  public void setup(VectorAccessible incoming, VectorContainer outgoing) {
    manifestEntryProcessor.setup(incoming, outgoing);
  }

  public void setupManifestFile(ManifestFile manifestFile, int row) {
    manifestReader = getManifestReader(manifestFile);
    if (manifestScanFilters.doesIcebergAnyColExpressionExists()) {
      manifestReader.filterRows(manifestScanFilters.getIcebergAnyColExpressionDeserialized());
    }

    iterator = DremioManifestReaderUtils.liveManifestEntriesIterator(manifestReader).iterator();
    applyManifestScanFilters(manifestFile);

    manifestEntryProcessor.initialise(manifestReader.spec(), row);
  }

  private void applyManifestScanFilters(ManifestFile manifestFile) {
    // Primarily used by the compaction operation (OPTIMIZE TABLE), to filter down rewritable files.
    if (manifestScanFilters == null) {
      return;
    }

    if (manifestScanFilters.doesMinPartitionSpecIdExist()
        && manifestFile.partitionSpecId() < manifestScanFilters.getMinPartitionSpecId()) {
      return; // Read all files as they belong to an old partition.
    }

    // Skip the data files if they fall within the given range
    if (manifestScanFilters.doesSkipDataFileSizeRangeExist()) {
      iterator =
          new FilterIterator<ManifestEntryWrapper<?>>(
              (CloseableIterator<ManifestEntryWrapper<?>>) iterator) {
            @Override
            protected boolean shouldKeep(ManifestEntryWrapper<?> dataFile) {
              return manifestScanFilters
                  .getSkipDataFileSizeRange()
                  .isNotInRange(dataFile.file().fileSizeInBytes());
            }
          };
    }
  }

  public int process(int startOutIndex, int maxOutputCount) throws Exception {
    int currentOutputCount = 0;
    while ((currentManifestEntry != null || iterator.hasNext())
        && currentOutputCount < maxOutputCount) {
      if (currentManifestEntry == null) {
        nextDataFile();
      }
      int outputRecords =
          manifestEntryProcessor.processManifestEntry(
              currentManifestEntry,
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
    AutoCloseables.close(manifestEntryProcessor);
  }

  @VisibleForTesting
  ManifestReader<? extends ContentFile<?>> getManifestReader(ManifestFile manifestFile) {
    if (manifestFile.content() == ManifestContent.DATA) {
      return ManifestFiles.read(manifestFile, getFileIO(manifestFile), partitionSpecMap);
    } else {
      return ManifestFiles.readDeleteManifest(
          manifestFile, getFileIO(manifestFile), partitionSpecMap);
    }
  }

  private FileIO getFileIO(ManifestFile manifestFile) {

    FileSystem fs = createFs(manifestFile.path(), context, opProps, icebergRootPointerPlugin);
    Preconditions.checkState(fs != null, "Unexpected state");
    return icebergRootPointerPlugin.createIcebergFileIO(
        fs, context, dataset, datasourcePluginUID, manifestFile.length());
  }

  private void nextDataFile() {
    currentManifestEntry = iterator.next();
    incrementFileCountMetric();
  }

  private void incrementFileCountMetric() {
    TableFunctionOperator.Metric metric;
    FileContent content = currentManifestEntry.file().content();
    switch (content) {
      case DATA:
        metric = TableFunctionOperator.Metric.NUM_DATA_FILE;
        break;
      case POSITION_DELETES:
        metric = TableFunctionOperator.Metric.NUM_POS_DELETE_FILES;
        break;
      case EQUALITY_DELETES:
        metric = TableFunctionOperator.Metric.NUM_EQ_DELETE_FILES;
        break;
      default:
        throw new IllegalStateException(String.format("Unknown FileContent type: %s", content));
    }
    operatorStats.addLongStat(metric, 1);
  }

  private void resetCurrentDataFile() {
    currentManifestEntry = null;
    manifestEntryProcessor.closeManifestEntry();
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

  private static FileSystem createFs(
      String path,
      OperatorContext context,
      OpProps props,
      SupportsIcebergRootPointer icebergRootPointerPlugin) {
    try {
      return icebergRootPointerPlugin.createFSWithAsyncOptions(path, props.getUserName(), context);
    } catch (IOException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  private static List<String> getDataset(TableFunctionConfig functionConfig) {
    TableFunctionContext functionContext = functionConfig.getFunctionContext();
    Collection<List<String>> referencedTables = functionContext.getReferencedTables();
    return CollectionUtils.isEmpty(referencedTables) ? null : referencedTables.iterator().next();
  }

  private static Configuration getConfiguration(SupportsIcebergRootPointer fileSystemPlugin) {
    return fileSystemPlugin.getFsConfCopy();
  }

  private SupportsIcebergRootPointer getStoragePlugin(
      FragmentExecutionContext fec, TableFunctionContext functionContext) {
    StoragePluginId pluginId = getPluginId(functionContext);
    try {
      return fec.getStoragePlugin(pluginId);
    } catch (ExecutionSetupException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }
}
