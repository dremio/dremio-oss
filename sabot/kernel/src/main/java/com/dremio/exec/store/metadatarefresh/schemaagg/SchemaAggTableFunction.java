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
package com.dremio.exec.store.metadatarefresh.schemaagg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.exception.NoSupportedUpPromotionOrCoercionException;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Table function which given a list of BatchSchema's will merge them into one final
 * batch schema applying up promotions for fields if needed.
 *
 * Input Vector:
 *  1) A VarBinaryVector with filed name #{@link MetadataRefreshExecConstants.SchemaAgg.INPUT_SCHEMA}
 *     which should have serialized batch schema's
 *
 * Output Vector -
 *  1) A varbinaryVector of size 1 with the merged schema
 */
public class SchemaAggTableFunction extends AbstractTableFunction implements SupportsTypeCoercionsAndUpPromotions {
  private static final Logger logger = LoggerFactory.getLogger(SchemaAggTableFunction.class);

  private VarBinaryVector outputFileSchemaVector;
  private VarBinaryVector inputFileSchemaVector;
  private BatchSchema reconciledSchema = new BatchSchema(Collections.EMPTY_LIST);
  private BatchSchema currentSchema;
  private boolean processedRow;
  private List<TransferPair> transferPairs = new ArrayList<>();

  public SchemaAggTableFunction(OperatorContext context, TableFunctionConfig functionConfig) {
    super(context, functionConfig);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    this.incoming = accessible;
    this.inputFileSchemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);

    this.outgoing = context.createOutputVectorContainer(incoming.getSchema());
    List<Field> fieldList = incoming.getSchema().getFields();

    Optional<Field> schemaField = fieldList.stream()
      .filter(field -> field.getName().equals(MetadataRefreshExecConstants.SchemaAgg.INPUT_SCHEMA.SCHEMA))
      .findFirst();

    if(!schemaField.isPresent()) {
      throw new IllegalStateException(String.format("%s having schema not found in the input list of fields provided", MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA));
    }

    incoming.forEach(vw -> {
      String fieldName = vw.getField().getName();
      if (!fieldName.equals(MetadataRefreshExecConstants.SchemaAgg.INPUT_SCHEMA.SCHEMA)){
        transferPairs.add(
          vw.getValueVector().makeTransferPair(VectorUtil.getVectorFromSchemaPath(outgoing, fieldName)));
      }
    });

    this.outputFileSchemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);
    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    this.currentSchema = getBatchSchemaFrom(row);
    logger.debug("Processing row {}", row);
    this.processedRow = false;
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if(this.processedRow) {
      return 0;
    }

    logger.debug("Processing schema {}", currentSchema.toJSONString());
    //Will just try and reconcile the schema
    try {
      this.reconciledSchema = reconciledSchema.mergeWithUpPromotion(currentSchema, this);
    } catch (NoSupportedUpPromotionOrCoercionException e) {
      throw UserException.unsupportedError(e).message(e.getMessage()).build(logger);
    }
    logger.debug("Merged schema after processing row {} is {}", startOutIndex, reconciledSchema.toJSONString());
    if (reconciledSchema.getTotalFieldCount() > context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX)) {
      throw new ColumnCountTooLargeException((int) context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
    }

    this.processedRow = true;
    //Output table function
    if(startOutIndex == inputFileSchemaVector.getValueCount() - 1 || maxRecords == 1) {
      logger.debug("Writing final reconciledSchema to the output. ReconciledSchema = {}", reconciledSchema.toJSONString());
      this.outputFileSchemaVector.setSafe(0, this.reconciledSchema.serialize());
      this.outputFileSchemaVector.setValueCount(1);
      transferPairs.forEach(TransferPair::transfer);
    }
    return 1;
  }

  @Override
  public void closeRow() throws Exception {

  }

  private BatchSchema getBatchSchemaFrom(int index) {
    byte[] bytes = inputFileSchemaVector.get(index);
    if(bytes == null) {
      throw new IllegalStateException(String.format("Schema not found at index %s of %s vector", index, inputFileSchemaVector.getField().getName()));
    }
    return BatchSchema.deserialize(bytes);
  }
}
