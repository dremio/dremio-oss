/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.model.sources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.ValueVector;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.dac.model.common.NamespacePath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataFragmentWrapper;
import com.dremio.dac.model.job.ReleaseAfterSerialization;
import com.dremio.dac.service.errors.PhysicalDatasetNotFoundException;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobDataFragmentImpl;
import com.dremio.service.jobs.RecordBatchHolder;
import com.dremio.service.jobs.RecordBatches;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;

/**
 * A resource focused on guessing, previewing and applying formats to files and folders.
 */
@Options
public class FormatTools {

  public static final BooleanValidator FAST_PREVIEW = new BooleanValidator("dac.format.preview.fast_preview", true);
  public static final PositiveLongValidator MIN_RECORDS = new PositiveLongValidator("dac.format.preview.min_records", Integer.MAX_VALUE, 1);
  public static final PositiveLongValidator MAX_READTIME_MS = new PositiveLongValidator("dac.format.preview.max_ms", Integer.MAX_VALUE, 500);
  public static final PositiveLongValidator BATCH_SIZE = new PositiveLongValidator("dac.format.preview.batch_size", Integer.MAX_VALUE, 20);
  public static final PositiveLongValidator TARGET_RECORDS = new PositiveLongValidator("dac.format.preview.target", Integer.MAX_VALUE, 200);

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatTools.class);

  private final SourceService sourceService;
  private final CatalogService catalogService;
  private final SecurityContext securityContext;
  private final BufferAllocator allocator;
  private final SabotContext context;

  @Inject
  public FormatTools(
      SourceService sourceService,
      CatalogService catalogService,
      SecurityContext securityContext,
      ContextService context) {
    super();
    this.sourceService = sourceService;
    this.catalogService = catalogService;
    this.securityContext = securityContext;
    this.allocator = context.get().getAllocator();
    this.context = context.get();
  }

  /**
   * Given a particular path, either return an existing format or attempt to detect the format.
   * @param folderPath
   * @return
   * @throws NamespaceException
   */
  public FileFormat getOrDetectFormat(NamespacePath folderPath, DatasetType expectedType) throws NamespaceException {
    // folder
    final FileFormat fileFormat;
    try {
      final PhysicalDatasetConfig physicalDatasetConfig = sourceService.getFilesystemPhysicalDataset(folderPath, expectedType);

      if (!expectedType.equals(physicalDatasetConfig.getType())) {
        throw new IllegalStateException(
            String.format(
                "Expected format of type %s but actually of format %s.",
                expectedType,
                physicalDatasetConfig.getType()
                )
            );
      }

      // determine whether folder or file.
      final boolean isFolder;
      switch(physicalDatasetConfig.getType()) {
        case PHYSICAL_DATASET_HOME_FILE:
        case PHYSICAL_DATASET_SOURCE_FILE:
          isFolder = false;
          break;
        case PHYSICAL_DATASET_SOURCE_FOLDER:
        case PHYSICAL_DATASET_HOME_FOLDER:
          isFolder = true;
          break;
        case PHYSICAL_DATASET:
        case VIRTUAL_DATASET:
        default:
          throw new IllegalStateException("Dataset is neither a file nor a folder.");
      }

      final FileConfig fileConfig = physicalDatasetConfig.getFormatSettings();
      fileFormat = isFolder ? FileFormat.getForFolder(fileConfig) : FileFormat.getForFile(fileConfig);
      fileFormat.setVersion(physicalDatasetConfig.getTag());
      return fileFormat;
    } catch (PhysicalDatasetNotFoundException nfe) {
      // ignore and fall through to detect the format so we don't have extra nested blocks.
    }

    final NamespaceKey key = folderPath.toNamespaceKey();
    return detectFileFormat(key);
  }

  private FileFormat detectFileFormat(NamespaceKey key) {
    final FileSystemPlugin plugin = getPlugin(key);
    final FileSystemWrapper fs = plugin.createFS(securityContext.getUserPrincipal().getName());
    final Path path = FileSelection.getPathBasedOnFullPath(plugin.resolveTableNameToValidPath(key.getPathComponents()));

    // for now, existing rudimentary behavior that uses extension detection.
    final FileStatus status;
    try {
      status = fs.getFileStatus(path);
    } catch(IOException ex) {
      // we could return unknown but if there no files, what's the point.
      throw UserException.ioExceptionError(ex)
        .message("No files detected or unable to read file format with selected option.")
        .build(logger);
    }

    if(status.isFile()) {
      return asFormat(key, path, false);
    }

    // was something other than file.
    try {
      Iterator<FileStatus> iter = getFilesForFormatting(fs, path);
      while(iter.hasNext()) {
        FileStatus child = iter.next();
        if(!child.isFile()) {
          continue;
        }

        return asFormat(key, child.getPath(), true);
      }

      // if we fall through, we didn't find any files.
      throw UserException.ioExceptionError()
      .message("No files were found.")
      .build(logger);
    } catch (IOException ex) {
      throw UserException.ioExceptionError(ex)
        .message("Unable to read file with selected format.")
        .build(logger);

    }
  }

  private static Iterator<FileStatus> getFilesForFormatting(FileSystemWrapper fs, Path path) throws IOException{
    return fs.listRecursive(path, false).iterator();
  }

  private static FileFormat asFormat(NamespaceKey key, Path path, boolean isFolder) {

    String name = path.getName();
    if(name.endsWith(".zip")) {
      name = name.substring(0, name.length() - 4);
    }

    if(name.endsWith(".gz")) {
      name = name.substring(0, name.length() - 3);
    }

    final FileConfig config = new FileConfig()
        .setCtime(System.currentTimeMillis())
        .setFullPathList(key.getPathComponents())
        .setName(key.getName())
        .setType(FileFormat.getFileFormatType(Collections.singletonList(FilenameUtils.getExtension(name))))
        .setTag(null);
    return isFolder ? FileFormat.getForFolder(config) : FileFormat.getForFile(config);
  }

  public JobDataFragment previewData(FileFormat format, NamespacePath namespacePath, boolean useFormatLocation) {
    final NamespaceKey key = namespacePath.toNamespaceKey();
    final FileSystemPlugin plugin = getPlugin(key);
    final FileSystemWrapper fs = plugin.createFS(securityContext.getUserPrincipal().getName());
    final Path path = FileSelection.getPathBasedOnFullPath(plugin.resolveTableNameToValidPath(key.getPathComponents()));

    // for now, existing rudimentary behavior that uses extension detection.
    final FileStatus status;
    try {
      status = fs.getFileStatus(path);
    } catch(IOException ex) {
      // we could return unknown but if there no files, what's the point.
      throw new IllegalStateException("No files detected or unable to read data.", ex);
    }

    try {

      if(status.isFile()) {
        return getData(format, plugin, fs, new NextableSingleton<>(status));
      }

      Iterator<FileStatus> iter = getFilesForFormatting(fs, path);
      return getData(
          format,
          plugin,
          fs,
          new PredicateNextable<>(
              () -> iter.hasNext() ? iter.next() : null,
              f -> f.isFile()
              )
          );
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  private JobDataFragment getData(FileFormat format, FileSystemPlugin plugin, FileSystemWrapper filesystem, Nextable<? extends FileStatus> files) throws Exception {

    final int minRecords = (int) context.getOptionManager().getOption(MIN_RECORDS);
    final long maxReadTime = context.getOptionManager().getOption(MAX_READTIME_MS);
    final int batchSize = (int) context.getOptionManager().getOption(BATCH_SIZE);
    final int targetRecords = (int) context.getOptionManager().getOption(TARGET_RECORDS);
    final int maxLeafColumns = (int) context.getOptionManager().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX);

    // we need to keep reading data until we get to target records. This could happen on the first scanner or take many.

    final FormatPluginConfig formatPluginConfig = PhysicalDatasetUtils.toFormatPlugin(format.asFileConfig(), Collections.<String>emptyList());
    final FormatPlugin formatPlugin = plugin.getFormatPlugin(formatPluginConfig);
    ReleasingData data = null;

    // everything in here will be released on serialization if success. If failure, will release in finally.
    try (RollbackCloseable cls = new RollbackCloseable(true)) {

      final OperatorContextImpl opCtxt = cls.add(
          new OperatorContextImpl(
              context.getConfig(),
              allocator.newChildAllocator("job-serialize", 0, Long.MAX_VALUE),
              context.getOptionManager(), batchSize));
      final BufferAllocator readerAllocator = opCtxt.getAllocator();
      final VectorContainer container = cls.add(new VectorContainer(readerAllocator));
      final Map<String, ValueVector> fieldVectorMap = new HashMap<>();
      final OutputMutator mutator = new ScanOperator.ScanMutator(container, fieldVectorMap, opCtxt, new SchemaChangeCallBack());
      final RBDList batches = cls.add(new RBDList(container, readerAllocator));
      int records = 0;

      boolean first = true;

      final Stopwatch timer = Stopwatch.createStarted();
      readersLoop: while(
          records < targetRecords
          && !(records > minRecords && timer.elapsed(TimeUnit.MILLISECONDS) > maxReadTime)
          ) { // loop for each record reader.
        FileStatus status = files.next();
        if(status == null) {
          break;
        }


        try (
          // Reader can be closed since data that we're using will be transferred to allocator owned by us
          // when added to RecordBatchData.
          RecordReader reader = formatPlugin.getRecordReader(opCtxt, filesystem, status)) {

          if (reader == null) {
            continue;
          }

          reader.setup(mutator);

          int output = 0;

          // keep adding batches from current reader until we get the most we need.
          // we have to keep doing this since otherwise the separate parquet row groups my not be read
          // (if each is smaller than target).
          while (
            records < targetRecords
              && !(records > minRecords && timer.elapsed(TimeUnit.MILLISECONDS) > maxReadTime)
            ) {

            reader.allocate(fieldVectorMap);
            output = reader.next();

            if (first) {
              first = false;
              container.buildSchema();
              if (container.getSchema().getTotalFieldCount() > maxLeafColumns) {
                throw UserException.validationError()
                    .message("Using datasets with more than %d columns is currently disabled.", maxLeafColumns)
                    .build(logger);
              }

              // call to reset state.
              mutator.isSchemaChanged();
            } else {
              if (mutator.isSchemaChanged()) {
                // let's stop early and not add this data. For format preview we'll just show data until a schema change.
                break readersLoop;
              }
            }

            if (output == 0) {
              break;
            }

            container.setRecordCount(output);
            records += output;
            batches.add();
          }
        }
      }

      data = new ReleasingData(cls, batches);

      cls.commit();
      return new JobDataFragmentWrapper(0, data);
    }
  }

  private static class ReleasingData extends JobDataFragmentImpl implements ReleaseAfterSerialization {

    private final RollbackCloseable closeable;

    public ReleasingData(RollbackCloseable closeable, RBDList rbdList) {
      super(rbdList.toRecordBatches(), 0, new JobId().setId("preview-job").setName("preview-job"));
      this.closeable = closeable;
    }

    @Override
    public synchronized void close() {
      List<AutoCloseable> closeables = new ArrayList<>();
      closeables.add(() -> super.close());
      List<AutoCloseable> backList = new ArrayList<>();
      backList.addAll(closeable.getCloseables());
      Collections.reverse(backList);
      closeables.addAll(backList);
      AutoCloseables.closeNoChecked(AutoCloseables.all(closeables));
    }
  }

  private class RBDList implements AutoCloseable {

    private final List<RecordBatchData> batches = new ArrayList<>();
    private final VectorAccessible accessible;
    private final BufferAllocator allocator;

    public RBDList(VectorAccessible accessible, BufferAllocator allocator) {
      super();
      this.accessible = accessible;
      this.allocator = allocator;
    }

    public void add() {
      RecordBatchData d = new RecordBatchData(accessible, allocator);
      batches.add(d);
    }

    @Override
    public void close() throws Exception {
      AutoCloseables.close(batches);
    }

    public RecordBatches toRecordBatches() {
      return new RecordBatches(batches.stream()
          .map(t -> RecordBatchHolder.newRecordBatchHolder(t, 0, t.getRecordCount()))
          .collect(Collectors.toList())
          );
    }
  }

  private final FileSystemPlugin getPlugin(NamespaceKey key) {
    StoragePlugin plugin = catalogService.getSource(key.getRoot());
    if(plugin instanceof FileSystemPlugin) {
      return (FileSystemPlugin) plugin;
    } else {
      throw UserException.validationError()
        .message("Source identified was invalid type. Only sources that can contain files or folders can detect a format.")
        .build(logger);
    }
  }

  private interface Nextable<T> {
    T next() throws IOException;
  }

  private class PredicateNextable<T> implements Nextable<T> {

    private final Nextable<T> delegate;
    private final Predicate<T> predicate;

    public PredicateNextable(Nextable<T> delegate, Predicate<T> predicate) {
      super();
      this.delegate = delegate;
      this.predicate = predicate;
    }

    @Override
    public T next() throws IOException {

      T next = delegate.next();
      while(next != null) {
        if(predicate.test(next)) {
          return next;
        } else {
          next = delegate.next();
        }
      }

      return null;
    }

  }

  private class NextableSingleton<T> implements Nextable<T> {

    public NextableSingleton(T singleValue) {
      super();
      this.singleValue = singleValue;
    }

    private T singleValue;

    @Override
    public T next() throws IOException {
      T tmp = singleValue;
      singleValue = null;
      return tmp;
    }

  }

}
