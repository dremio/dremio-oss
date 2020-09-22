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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.DataFile;

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
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.IcebergOperation;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

/**
 * SqlOperatorImpl responsible for moving the write directory from a private location
 * to a public location for future query. Also responsible for storing a set of
 * files and their associated partitions and schema.
 */
public class WriterCommitterOperator implements SingleInputOperator {

  private final WriterCommitterPOP config;
  private final OperatorContext context;
  private FileSystem fs;

  private long recordCount;
  private ProjectOperator project;
  private boolean success = false;
  private IcebergOpCommitter icebergOpCommitter;
  private final List<ValueVector> vectors = new ArrayList<>();
  private boolean icebergTableCommitter = false;
  private VarBinaryVector icebergMetadataVector;

  public WriterCommitterOperator(OperatorContext context, WriterCommitterPOP config){
    this.config = config;
    this.context = context;
    icebergOpCommitter = null;
    icebergTableCommitter = config.getIcebergTableProps() != null;
  }

  @Override
  public State getState(){
    return project == null ? State.NEEDS_SETUP : project.getState();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    for (VectorWrapper<?> vectorWrapper : accessible) {
      vectors.add(vectorWrapper.getValueVector());
    }
    final BatchSchema schema = accessible.getSchema();
    if(!schema.equals(RecordWriter.SCHEMA)){
      throw new IllegalStateException(String.format("Incoming record writer schema doesn't match intended. Expected %s, received %s", RecordWriter.SCHEMA, schema));
    }

    fs = config.getPlugin().createFS(config.getProps().getUserName());

    if (icebergTableCommitter) {
      IcebergTableProps icebergTableProps = config.getIcebergTableProps();
      TypedFieldId id = RecordWriter.SCHEMA.getFieldId(SchemaPath.getSimplePath(RecordWriter.ICEBERG_METADATA_COLUMN));
      icebergMetadataVector = accessible.getValueAccessorById(VarBinaryVector.class, id.getFieldIds()).getValueVector();

      switch (icebergTableProps.getIcebergOpType()) {
        case CREATE:
          icebergOpCommitter = IcebergOperation.getCreateTableCommitter(
            Path.of(icebergTableProps.getTableLocation()),
            icebergTableProps.getFullSchema(),
            icebergTableProps.getPartitionColumnNames(),
            config.getPlugin().getFsConfCopy());
          break;
        case INSERT:
          icebergOpCommitter = IcebergOperation.getInsertTableCommitter(
            Path.of(icebergTableProps.getTableLocation()),
            icebergTableProps.getFullSchema(),
            icebergTableProps.getPartitionColumnNames(),
            config.getPlugin().getFsConfCopy());
          break;
      }
    }

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
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.ICEBERG_METADATA.getName()), new FieldReference(RecordWriter.ICEBERG_METADATA.getName()))
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
      icebergTableCommitter = false;
      icebergOpCommitter = null;
      if (!success) {
        Path path = Path.of(Optional.fromNullable(config.getTempLocation()).or(config.getFinalLocation()));
        if (fs != null && fs.exists(path)) {
          fs.delete(path, true);
        }
      }
    } finally {
        AutoCloseables.close(project, fs);
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
    if (icebergTableCommitter) {
      if (icebergOpCommitter != null) {
        // iceberg library isn't using dremio's fs wrappers. So, accounting the
        // entire call as wait time.
        try (AutoCloseable ac = OperatorStats.getWaitRecorder(context.getStats())) {
          icebergOpCommitter.commit();
        }
      }
    }
    success = true;
  }

  @Override
  public void consumeData(int records) throws Exception {
    project.consumeData(records);
    if (icebergTableCommitter) {
      List<DataFile> icebergDatafiles = new ArrayList<>();
      for (int i = 0; i < records; ++i) {
        DataFile dataFile = IcebergSerDe.deserializeDataFile(icebergMetadataVector.get(i));
        icebergDatafiles.add(dataFile);
      }
      if (icebergDatafiles.size() > 0) {
        try (AutoCloseable ac = OperatorStats.getWaitRecorder(context.getStats())) {
          icebergOpCommitter.consumeData(icebergDatafiles);
        }
      }
    }
    recordCount += records;
  }

  public static class WriterCreator implements SingleInputOperator.Creator<WriterCommitterPOP>{

    @Override
    public SingleInputOperator create(OperatorContext context, WriterCommitterPOP operator) throws ExecutionSetupException {
      return new WriterCommitterOperator(context, operator);
    }

  }
}
