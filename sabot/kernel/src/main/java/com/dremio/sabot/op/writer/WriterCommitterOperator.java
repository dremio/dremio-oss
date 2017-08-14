/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
import com.dremio.exec.store.RecordWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
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

  public WriterCommitterOperator(OperatorContext context, WriterCommitterPOP config){
    this.config = config;
    this.context = context;
  }

  public State getState(){
    return project == null ? State.NEEDS_SETUP : project.getState();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    final BatchSchema schema = accessible.getSchema();
    if(!schema.equals(RecordWriter.SCHEMA)){
      throw new IllegalStateException(String.format("Incoming record writer schema doesn't match intended. Expected %s, received %s", RecordWriter.SCHEMA, schema));
    }

    fs = config.getPlugin().getFS(config.getUserName());

    // replacement expression.
    FunctionCall replacement = new FunctionCall("REGEXP_REPLACE", ImmutableList.<LogicalExpression>of(
        SchemaPath.getSimplePath(RecordWriter.PATH.getName()),
        new ValueExpressions.QuotedString(Pattern.quote(config.getTempLocation())),
        new ValueExpressions.QuotedString(config.getFinalLocation())
        ));

    Project projectConfig = new Project(ImmutableList.of(
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.FRAGMENT.getName()), new FieldReference(RecordWriter.FRAGMENT.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.RECORDS.getName()), new FieldReference(RecordWriter.RECORDS.getName())),
        new NamedExpression(replacement, new FieldReference(RecordWriter.PATH.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.METADATA.getName()), new FieldReference(RecordWriter.METADATA.getName())),
        new NamedExpression(SchemaPath.getSimplePath(RecordWriter.PARTITION.getName()), new FieldReference(RecordWriter.PARTITION.getName()))
        ), null);
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
      if (!success) {
        Path temp = new Path(config.getTempLocation());
        if (fs != null && fs.exists(temp)) {
          fs.delete(temp, true);
        }
      }
    } finally {
        AutoCloseables.close(project, fs);
    }
  }

  @Override
  public void noMoreToConsume() throws Exception {
    Path temp = new Path(config.getTempLocation());
    if(fs.exists(temp)){
      fs.rename(temp, new Path(config.getFinalLocation()));
    }
    project.noMoreToConsume();
    success = true;
  }

  @Override
  public void consumeData(int records) throws Exception {
    project.consumeData(records);
    recordCount += records;
  }

  public static class WriterCreator implements SingleInputOperator.Creator<WriterCommitterPOP>{

    @Override
    public SingleInputOperator create(OperatorContext context, WriterCommitterPOP operator) throws ExecutionSetupException {
      return new WriterCommitterOperator(context, operator);
    }

  }
}
