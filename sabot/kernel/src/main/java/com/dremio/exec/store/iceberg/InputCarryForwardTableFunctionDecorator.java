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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.BasePath;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.tablefunction.TableFunction;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;

/**
 * Uses carry-forward columns to accumulate input rows.
 *
 * a. If carry-forward columns are present in the incoming schema, the rows which have these columns populated are
 *    copied over as they are into output.
 * b. Carry-forward column values from the incoming row are set in a carry-forward row, as per a mapping rule.
 */
public class InputCarryForwardTableFunctionDecorator implements TableFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(InputCarryForwardTableFunctionDecorator.class);
  private final TableFunction baseTableFunction;
  private final List<String> carryForwardCols;
  private final Map<SchemaPath, SchemaPath> mappingRule;
  private final String inputTypeCol;
  private final String inputType;
  private List<TransferPair> carryForwardColTransfers;
  private List<TransferPair> mappingColTransfers = new ArrayList<>();
  private List<ValueVector> carryForwardColVectors;
  private VarCharVector inputTypeOutVector;
  private boolean isCarryForwardRow = false;
  private boolean mappingRuleProcessed = false;
  private boolean rowCompleted = false;
  private int row;
  private VectorContainer outgoing;

  public InputCarryForwardTableFunctionDecorator(TableFunction baseTableFunction, List<String> carryForwardCols,
                                                 Map<SchemaPath, SchemaPath> mappingRule, String inputTypeCol, String inputType) {
    Preconditions.checkState(!carryForwardCols.isEmpty());
    this.baseTableFunction = baseTableFunction;
    this.carryForwardCols = carryForwardCols;
    this.mappingRule = mappingRule;

    // Feed constant value "inputType" to the "inputTypeCol"
    this.inputTypeCol = inputTypeCol;
    this.inputType = inputType;
    this.mappingRuleProcessed = mappingRule.isEmpty(); // No need to process the mapping rule
  }

  @Override
  public VectorAccessible setup(VectorAccessible incoming) throws Exception {
    this.outgoing = (VectorContainer) baseTableFunction.setup(incoming);

    // create transfer pairs for any additional input columns
    carryForwardColTransfers = Streams.stream(incoming)
      .filter(vw -> carryForwardCols.contains(vw.getValueVector().getName()) &&
        outgoing.getSchema().getFieldId(BasePath.getSimple(vw.getValueVector().getName())) != null)
      .map(vw -> vw.getValueVector().makeTransferPair(
        getVectorFromSchemaPath(outgoing, vw.getValueVector().getName())))
      .collect(Collectors.toList());

    mappingRule.keySet().forEach(inCol -> Preconditions.checkNotNull(incoming.getSchema().getFieldId(inCol),
      String.format("Mapping rule source columns not found [required: %s]", mappingRule)));
    mappingRule.values().forEach(outCol -> Preconditions.checkNotNull(outgoing.getSchema().getFieldId(outCol),
      String.format("Mapping rule carry-forwarding columns not found in the outgoing schema [required: %s]", mappingRule)));

    for (Map.Entry<SchemaPath, SchemaPath> mappedInputCol : mappingRule.entrySet()) {
      ValueVector mapInputVector = getVector(incoming, mappedInputCol.getKey());
      ValueVector mapOutVector = getVector(outgoing, mappedInputCol.getValue());
      if (mapInputVector != null && mapOutVector != null) {
        mappingColTransfers.add(mapInputVector.makeTransferPair(mapOutVector));
      }
    }

    carryForwardColVectors = Streams.stream(incoming).filter(vw -> carryForwardCols.contains(vw.getValueVector().getName()))
      .map(VectorWrapper::getValueVector).collect(Collectors.toList());

    inputTypeOutVector = (VarCharVector) getVectorFromSchemaPath(outgoing, inputTypeCol);

    return outgoing;
  }

  public static ValueVector getVector(VectorAccessible vectorAccessible, SchemaPath schemaPath) {
    TypedFieldId typedFieldId = vectorAccessible.getSchema().getFieldId(schemaPath);
    Field field = vectorAccessible.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    return vectorAccessible.getValueAccessorById(TypeHelper.getValueVectorClass(field), typedFieldId.getFieldIds()).getValueVector();
  }

  @Override
  public void startBatch(int records) throws Exception {
    baseTableFunction.startBatch(records);
  }

  @Override
  public void startRow(int row) throws Exception {
    this.isCarryForwardRow = (carryForwardColVectors.stream().anyMatch(vv -> !vv.isNull(row)));
    this.row = row;
    if (!isCarryForwardRow) {
      baseTableFunction.startRow(row);
    }
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if (rowCompleted) {
      return 0;
    }

    // For a carry-forward row, we directly copy the values from input vectors to output vectors on those carry-forward columns.
    // This helps to keep those values in output vectors not changed.
    if (isCarryForwardRow) {
      carryForwardColTransfers.forEach(tx -> tx.copyValueSafe(row, startOutIndex));
      rowCompleted = true;
      outgoing.setRecordCount(startOutIndex + 1);
      outgoing.forEach(vw -> vw.getValueVector().setValueCount(startOutIndex + 1));
      return 1;
    }

    int outIdx = startOutIndex;
    int records = 0;
    // Mapping rule appends one more row into output. This row helps to inject file path and type info that baseFunction
    // needs to process. For instance, if the baseFunction is ManifestScanTF and processes a manifest file, it will inject
    // the manifest file path and type into outputs. So that, we can inject and carry forward manifest file info.
    if (!mappingRuleProcessed) {
      int finalOutIdx = outIdx;
      mappingColTransfers.forEach(tx -> tx.copyValueSafe(row, finalOutIdx));
      inputTypeOutVector.setSafe(outIdx, inputType.getBytes(StandardCharsets.UTF_8));
      outgoing.forEach(vw -> vw.getValueVector().setValueCount(finalOutIdx + 1));
      outgoing.setRecordCount(++outIdx);
      mappingRuleProcessed = true;
      records++;
    }
    int recordsBase = baseTableFunction.processRow(outIdx, maxRecords - records);
    records += recordsBase;
    LOGGER.debug("[IN:{}, row{}, out{}], base-func_processed:{}, total-records:{}, outgoing-record_cnt {}",
      baseTableFunction.getClass().getSimpleName(), row, startOutIndex, recordsBase, records, outgoing.getRecordCount());
    return records;
  }

  @Override
  public void closeRow() throws Exception {
    if (!isCarryForwardRow) {
      baseTableFunction.closeRow();
    }
    rowCompleted = false;
    isCarryForwardRow = false;
    mappingRuleProcessed = mappingRule.isEmpty();
  }

  @Override
  public boolean hasBufferedRemaining() {
    return baseTableFunction.hasBufferedRemaining();
  }

  @Override
  public void workOnOOB(OutOfBandMessage message) {
    baseTableFunction.workOnOOB(message);
  }

  @Override
  public long getFirstRowSize() {
    return baseTableFunction.getFirstRowSize();
  }

  @Override
  public void noMoreToConsume() throws Exception {
    baseTableFunction.noMoreToConsume();
  }

  @Override
  public void close() throws Exception {
    baseTableFunction.close();
  }
}
