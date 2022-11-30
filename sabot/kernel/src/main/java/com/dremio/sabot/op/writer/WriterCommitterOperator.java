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
package com.dremio.sabot.op.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.arrow.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.physical.config.Project;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.iceberg.manifestwriter.IcebergCommitOpHelper;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;


/**
 * SqlOperatorImpl responsible for moving the write directory from a private location
 * to a public location for future query. Also responsible for storing a set of
 * files and their associated partitions and schema.
 */
public class WriterCommitterOperator implements SingleInputOperator {

  public enum Metric implements MetricDef {
    ICEBERG_METADATA_WRITE_TIME,
    ICEBERG_CATALOG_UPDATE_TIME,
    ICEBERG_COMMIT_TIME,
    ICEBERG_COMMIT_SETUP_TIME,
    ICEBERG_COMMAND_CREATE_TIME,
    COMMITTER_CLOSE_TIME,
    FILE_SYSTEM_CREATE_TIME,
    READ_SIGNATURE_COMPUTE_TIME,
    TABLE_RENAME_TIME,
    MIN_IO_READ_TIME_NS,   // Minimum IO read time
    MAX_IO_READ_TIME_NS,   // Maximum IO read time
    AVG_IO_READ_TIME_NS,   // Average IO read time
    NUM_IO_READ,        // Total Number of IO reads
    MIN_METADATA_IO_READ_TIME_NS,  // Minimum IO read time for metadata operations
    MAX_METADATA_IO_READ_TIME_NS,   // Maximum IO read time for metadata operations
    AVG_METADATA_IO_READ_TIME_NS,  // Average IO read time for metadata operations
    NUM_METADATA_IO_READ,
    MIN_IO_WRITE_TIME, // Minimum IO write time
    MAX_IO_WRITE_TIME, // Maximum IO write time
    AVG_IO_WRITE_TIME, // Avg IO write time
    NUM_IO_WRITE,      // Total Number of IO writes
    ;
    @Override
    public int metricId() {
      return ordinal();
    }
  }

  protected static final Logger logger = LoggerFactory.getLogger(WriterCommitterOperator.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(WriterCommitterOperator.class);

  @VisibleForTesting
  public static final String INJECTOR_AFTER_NO_MORETO_CONSUME_ERROR = "error-between-noMoreToConsume-and-icebergeCommit";

  private final WriterCommitterPOP config;
  private final OperatorContext context;
  private FileSystem fs;
  private final ExecutionControls executionControls;

  private long recordCount;
  private ProjectOperator project;
  private boolean success = false;
  private final List<ValueVector> vectors = new ArrayList<>();

  private final IcebergCommitOpHelper icebergCommitHelper;

  public WriterCommitterOperator(OperatorContext context, WriterCommitterPOP config) {
    this.config = config;
    this.context = context;
    this.icebergCommitHelper = IcebergCommitOpHelper.getInstance(context, config);
    this.executionControls = context.getExecutionControls();
  }

  @Override
  public State getState() {
    return project == null ? State.NEEDS_SETUP : project.getState();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    for (VectorWrapper<?> vectorWrapper : accessible) {
      vectors.add(vectorWrapper.getValueVector());
    }
    final BatchSchema schema = accessible.getSchema();
    if (!schema.equals(RecordWriter.SCHEMA)) {
      throw new IllegalStateException(String.format("Incoming record writer schema doesn't match intended. Expected %s, received %s", RecordWriter.SCHEMA, schema));
    }

    Stopwatch stopwatch = Stopwatch.createStarted();
    fs = config.getPlugin().createFS(Optional.fromNullable(config.getTempLocation()).or(config.getFinalLocation()),
      config.getProps().getUserName(), context);
    addMetricStat(Metric.FILE_SYSTEM_CREATE_TIME, stopwatch.elapsed(TimeUnit.MILLISECONDS));

    icebergCommitHelper.setup(accessible);

    // replacement expression.
    LogicalExpression replacement;
    if (config.getTempLocation() != null) {
      replacement = new FunctionCall("REGEXP_REPLACE", ImmutableList.<LogicalExpression>of(
        SchemaPath.getSimplePath(RecordWriter.PATH.getName()),
        new ValueExpressions.QuotedString(Pattern.quote(config.getTempLocation())),
        new ValueExpressions.QuotedString(config.getFinalLocation())
      ));
    } else {
      replacement = SchemaPath.getSimplePath(RecordWriter.PATH.getName());
    }
    Project projectConfig = new Project(
      config.getProps(),
      null,
      ImmutableList.of(
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.FRAGMENT.getName()), new FieldReference(RecordWriter.FRAGMENT.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.RECORDS.getName()), new FieldReference(RecordWriter.RECORDS.getName())),
        new NamedExpression(replacement, new FieldReference(RecordWriter.PATH.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.METADATA.getName()), new FieldReference(RecordWriter.METADATA.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.PARTITION.getName()), new FieldReference(RecordWriter.PARTITION.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.FILESIZE.getName()), new FieldReference(RecordWriter.FILESIZE.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.ICEBERG_METADATA.getName()), new FieldReference(RecordWriter.ICEBERG_METADATA.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.FILE_SCHEMA.getName()), new FieldReference(RecordWriter.FILE_SCHEMA.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.PARTITION_DATA.getName()), new FieldReference(RecordWriter.PARTITION_DATA.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.OPERATION_TYPE.getName()), new FieldReference(RecordWriter.OPERATION_TYPE.getName()))
      ));
    this.project = new ProjectOperator(context, projectConfig);
    return project.setup(accessible);
  }

  @Override
  public int outputData() throws Exception {
    return project.outputData();
  }

  @Override
  public <OUT, IN, EXCEP extends Throwable> OUT accept(OperatorVisitor<OUT, IN, EXCEP> visitor, IN value) throws EXCEP {
    return visitor.visitSingleInput(this, value);
  }

  @Override
  public void close() throws Exception {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      cleanUpFiles();
    } catch (Exception e) {
      logger.warn("Cleanup of files in writer committer failed.", e);
    }
    finally {
      final OperatorStats operatorStats = context.getStats();
      AutoCloseables.close(project, fs, icebergCommitHelper);

      OperatorStats.IOStats readIOStats = operatorStats.getReadIOStats();
      if (readIOStats != null) {
        long minIOReadTime = readIOStats.minIOTime.longValue() <= readIOStats.maxIOTime.longValue() ? readIOStats.minIOTime.longValue() : 0;
        operatorStats.setLongStat(Metric.MIN_IO_READ_TIME_NS, minIOReadTime);
        operatorStats.setLongStat(Metric.MAX_IO_READ_TIME_NS, readIOStats.maxIOTime.longValue());
        operatorStats.setLongStat(Metric.AVG_IO_READ_TIME_NS, readIOStats.numIO.get() == 0 ? 0 : readIOStats.totalIOTime.longValue() / readIOStats.numIO.get());
        operatorStats.addLongStat(Metric.NUM_IO_READ, readIOStats.numIO.longValue());
        operatorStats.addSlowIoInfos(readIOStats.slowIOInfoList);
      }

      OperatorStats.IOStats  metadataIoStats = operatorStats.getMetadataReadIOStats();
      if (metadataIoStats != null) {
        long minMetadataIOReadTime = metadataIoStats.minIOTime.longValue() <= metadataIoStats.maxIOTime.longValue() ? metadataIoStats.minIOTime.longValue() : 0;
        operatorStats.setLongStat(Metric.MIN_METADATA_IO_READ_TIME_NS, minMetadataIOReadTime);
        operatorStats.setLongStat(Metric.MAX_METADATA_IO_READ_TIME_NS, metadataIoStats.maxIOTime.longValue());
        operatorStats.setLongStat(Metric.AVG_METADATA_IO_READ_TIME_NS, metadataIoStats.numIO.get() == 0 ? 0 : metadataIoStats.totalIOTime.longValue() / metadataIoStats.numIO.get());
        operatorStats.addLongStat(Metric.NUM_METADATA_IO_READ, metadataIoStats.numIO.longValue());
        operatorStats.addSlowMetadataIoInfos(metadataIoStats.slowIOInfoList);
      }

      OperatorStats.IOStats writeIOStats = operatorStats.getWriteIOStats();
      if (writeIOStats != null) {
        long minIOWriteTime = writeIOStats.minIOTime.longValue() <= writeIOStats.maxIOTime.longValue() ? writeIOStats.minIOTime.longValue() : 0;
        operatorStats.setLongStat(Metric.MIN_IO_WRITE_TIME, minIOWriteTime);
        operatorStats.setLongStat(Metric.MAX_IO_WRITE_TIME, writeIOStats.maxIOTime.longValue());
        operatorStats.setLongStat(Metric.AVG_IO_WRITE_TIME, writeIOStats.numIO.get() == 0 ? 0 : writeIOStats.totalIOTime.longValue() / writeIOStats.numIO.get());
        operatorStats.addLongStat(Metric.NUM_IO_WRITE, writeIOStats.numIO.longValue());
        operatorStats.addSlowIoInfos(writeIOStats.slowIOInfoList);
      }
      operatorStats.setSlowIoInfosInProfile();
      addMetricStat(Metric.COMMITTER_CLOSE_TIME, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  public void noMoreToConsume() throws Exception {
    if (config.getTempLocation() != null) {
      Path temp = Path.of(config.getTempLocation());
      Stopwatch stopwatch = Stopwatch.createStarted();
      if (fs.exists(temp)) {
        fs.rename(temp, Path.of(config.getFinalLocation()));
      }
      addMetricStat(Metric.TABLE_RENAME_TIME, stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
    project.noMoreToConsume();
    injector.injectChecked(executionControls, INJECTOR_AFTER_NO_MORETO_CONSUME_ERROR, UnsupportedOperationException.class);
    icebergCommitHelper.commit();
    success = true;
  }

  @Override
  public void consumeData(int records) throws Exception {
    project.consumeData(records);
    icebergCommitHelper.consumeData(records);
    recordCount += records;
  }

  public static class WriterCreator implements SingleInputOperator.Creator<WriterCommitterPOP> {

    @Override
    public SingleInputOperator create(OperatorContext context, WriterCommitterPOP operator) throws ExecutionSetupException {
      return new WriterCommitterOperator(context, operator);
    }
  }

  private void cleanUpIcebergTables() throws IOException, ClassNotFoundException {
      logger.info("Updating iceberg table failed. Cleaning up hanging manifest files");
      //Commit should not have happened so no vn.metadata.json. Only manifest files written by previous operator need to be cleaned up.
      icebergCommitHelper.cleanUpManifestFiles(fs);
  }

  private String getCleanupLocation() {
    if ( (config.getIcebergTableProps() != null) &&
          (config.getIcebergTableProps().getIcebergOpType() == IcebergCommandType.FULL_METADATA_REFRESH ||
              config.getIcebergTableProps().getIcebergOpType() == IcebergCommandType.CREATE) ) {
      return config.getIcebergTableProps().getTableLocation();
    }
    return Optional.fromNullable(config.getTempLocation()).or(config.getFinalLocation());
  }

  private void cleanUpFiles() throws IOException, ClassNotFoundException {
    if(config.getIcebergTableProps() == null ||
            (config.getIcebergTableProps().getIcebergOpType() == IcebergCommandType.FULL_METADATA_REFRESH) ||
            (config.getIcebergTableProps().getIcebergOpType() == IcebergCommandType.CREATE) ||
            (config.getIcebergTableProps().getIcebergOpType() == IcebergCommandType.INSERT)) {
      if(!success) {
        Path path = Path.of(getCleanupLocation());
          if (fs != null && fs.exists(path)) {
            fs.delete(path, true);
          }
      }
      return;
    }

    if(!success && config.getIcebergTableProps().getIcebergOpType() == IcebergCommandType.INCREMENTAL_METADATA_REFRESH){
      cleanUpIcebergTables();
    }
  }

  private void addMetricStat(Metric metric, long time) {
    if (context.getStats() != null) {
      context.getStats().addLongStat(metric, time);
    }
  }
}
