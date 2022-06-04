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
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
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
    ICEBERG_COMMIT_TIME;

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

    fs = config.getPlugin().createFS(config.getProps().getUserName());
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
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.PARTITION_DATA.getName()), new FieldReference(RecordWriter.PARTITION_DATA.getName()))
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
    try {
      cleanUpFiles();
    } catch (Exception e) {
      logger.warn("Cleanup of files in writer committer failed.", e);
    }
    finally {
      AutoCloseables.close(project, fs, icebergCommitHelper);
    }
  }

  @Override
  public void noMoreToConsume() throws Exception {
    if (config.getTempLocation() != null) {
      Path temp = Path.of(config.getTempLocation());
      if (fs.exists(temp)) {
        fs.rename(temp, Path.of(config.getFinalLocation()));
      }
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

  private void cleanUpFiles() throws IOException, ClassNotFoundException {
    if(config.getIcebergTableProps() == null || (config.getIcebergTableProps().getIcebergOpType() == IcebergCommandType.FULL_METADATA_REFRESH)) {
      if(!success) {
        Path path = Path.of(Optional.fromNullable(config.getTempLocation()).or(config.getFinalLocation()));
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
}
