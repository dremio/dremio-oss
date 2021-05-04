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
package com.dremio.exec.store.deltalake;

import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FIELD_ADD;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_PARTITION_VALUES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CastExpressionWithOverflow;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.project.SimpleProjector;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;

/**
 * Wrapper to coerce partition values in partitionValues field
 * Casts partition values from string type and transfers to corresponding top-level column in output mutator
 */
public class DeltaLogCommitJsonRecordReader implements RecordReader {

  private SimpleProjector projector;
  protected final RecordReader delegate;
  protected final OperatorContext context;
  private ScanOperator.ScanMutator outputMutator;
  private final List<Field> partitionCols;

  public DeltaLogCommitJsonRecordReader(OperatorContext context, RecordReader delegate, List<Field> partitionCols) {
    this.context = context;
    this.delegate = delegate;
    this.partitionCols = partitionCols;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = (ScanOperator.ScanMutator) output;
    delegate.setup(output);
    // setup projector for partition fields
    this.createAndSetupProjector(outputMutator.getContainer(), outputMutator.getContainer());
  }

  @Override
  public int next() {
    int count = delegate.next();
    projector.eval(count);
    return count;
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    delegate.allocate(vectorMap);
  }

  private List<NamedExpression> exprsToReadPartitionValues() {
    List<NamedExpression> exprs = new ArrayList<>();

    for (Field field : partitionCols) {
      final SchemaPath inputRef = // add.partitionValues.[columnName]
        SchemaPath.getCompoundPath(DELTA_FIELD_ADD, SCHEMA_PARTITION_VALUES, field.getName());

      final FieldReference outputRef = FieldReference.getWithQuotedRef(field.getName()); // top level partition column field

      TypeProtos.MajorType targetType =  MajorTypeHelper.getMajorTypeForField(field);

      LogicalExpression cast;
      if (targetType.getMinorType().equals(TypeProtos.MinorType.VARCHAR) || targetType.getMinorType().equals(TypeProtos.MinorType.VARBINARY)) {
        cast = inputRef;
      } else if (targetType.getMinorType().equals(TypeProtos.MinorType.DECIMAL)) {
        cast = new CastExpressionWithOverflow(inputRef, targetType);
      } else {
        cast = FunctionCallFactory.createCast(targetType, inputRef);
      }
      IfExpression.IfCondition parsedPartitionValueIsNotNull = new IfExpression.IfCondition(
              new FunctionCall("isnotnull", Collections.singletonList(inputRef)), inputRef);

      IfExpression ifEx = IfExpression.newBuilder()
              .setIfCondition(parsedPartitionValueIsNotNull)
              .setElse(cast)
              .build();

      exprs.add(new NamedExpression(ifEx, outputRef));
    }
    return exprs;
  }

  private void createAndSetupProjector(VectorContainer inputVectors, VectorContainer outputVectors) {
    projector = new SimpleProjector(context, inputVectors, this.exprsToReadPartitionValues(),
      outputVectors);
    projector.setup();
  }

  @Override
  public List<SchemaPath> getColumnsToBoost() {
    return delegate.getColumnsToBoost();
  }

  @Override
  public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
    delegate.addRuntimeFilter(runtimeFilter);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(delegate, projector);
  }
}
