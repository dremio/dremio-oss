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
package com.dremio.exec.planner.sql.handlers.query;

import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.SqlNodeUtil;
import com.dremio.exec.planner.sql.parser.SqlCreateTable;
import com.dremio.exec.store.AbstractSchema;
import com.dremio.exec.store.dfs.SchemaMutability.MutationType;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.google.common.collect.ImmutableMap;

public class CreateTableHandler implements SqlToPlanHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateTableHandler.class);

  private String textPlan;
  private final boolean systemUser;

  public CreateTableHandler(boolean systemUser) {
    this.systemUser = systemUser;
  }

  @Override
  public PhysicalPlan getPlan(SqlHandlerConfig config, String sql, SqlNode sqlNode) throws Exception {
    try{
      SqlCreateTable sqlCreateTable = SqlNodeUtil.unwrap(sqlNode, SqlCreateTable.class);
      final String newTblName = sqlCreateTable.getName();

      final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, sqlCreateTable.getQuery());
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();


      final RelNode newTblRelNode =
          SqlHandlerUtil.resolveNewTableRel(false, sqlCreateTable.getFieldNames(), validatedRowType, queryRelNode);

      final AbstractSchema schemaInstance =
          SchemaUtilities.resolveToMutableSchemaInstance(config.getConverter().getDefaultSchema(), sqlCreateTable.getSchemaPath(), systemUser, MutationType.TABLE);
      final String schemaPath = schemaInstance.getFullSchemaName();

      if (SqlHandlerUtil.getTableFromSchema(schemaInstance, newTblName) != null) {
        throw UserException.validationError()
            .message("A table or view with given name [%s] already exists in schema [%s]", newTblName, schemaPath)
            .build(logger);
      }

      final long ringCount = config.getContext().getOptions().getOption(PlannerSettings.RING_COUNT);

      final RelNode newTblRelNodeWithPCol = SqlHandlerUtil.qualifyPartitionCol(newTblRelNode, sqlCreateTable.getPartitionColumns());

      PrelTransformer.log("Calcite", newTblRelNodeWithPCol, logger, null);

      WriterOptions options = new WriterOptions(
        (int) ringCount,
        sqlCreateTable.getPartitionColumns(),
        sqlCreateTable.isHashPartition(),
        sqlCreateTable.isRoundRobinPartition(),
        sqlCreateTable.getSortColumns(),
        sqlCreateTable.getDistributionColumns());
      // Convert the query to Dremio Logical plan and insert a writer operator on top.
      Rel drel = convertToDrel(
          config,
          newTblRelNodeWithPCol,
          schemaInstance,
          newTblName,
          options,
          newTblRelNode.getRowType(),
          createStorageOptionsMap(config, sqlCreateTable.getFormatOptions()),
          sqlCreateTable.isSingleWriter());

      final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, drel);
      final Prel prel = convertToPrel.getKey();
      textPlan = convertToPrel.getValue();
      PhysicalOperator pop = PrelTransformer.convertToPop(config, prel);
      PhysicalPlan plan = PrelTransformer.convertToPlan(config, pop);
      PrelTransformer.log(config, "Dremio Plan", plan, logger);

      return plan;

    }catch(Exception ex){
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  private static Rel convertToDrel(
      SqlHandlerConfig config,
      RelNode relNode,
      AbstractSchema schema,
      String tableName,
      WriterOptions options,
      RelDataType queryRowType,
      final Map<String, Object> storageOptions,
      final boolean singleWriter)
      throws RelConversionException, SqlUnsupportedException {

    Rel convertedRelNode = PrelTransformer.convertToDrel(config, relNode);

    // Put a non-trivial topProject to ensure the final output field name is preserved, when necessary.
    // Only insert project when the field count from the child is same as that of the queryRowType.
    convertedRelNode = queryRowType.getFieldCount() == convertedRelNode.getRowType().getFieldCount() ?
        PrelTransformer.addRenamedProject(config, convertedRelNode, queryRowType) : convertedRelNode;

    convertedRelNode = SqlHandlerUtil.addWriterRel(convertedRelNode,
        schema.createNewTable(tableName, options, storageOptions), singleWriter);

    convertedRelNode = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(), config.getContext(), config.getConverter().getRootSchema(), convertedRelNode);

    return new ScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }


  /**
   * Helper method to create map of key, value pairs, the value is a Java type object.
   * @param args
   * @return
   */
  protected static Map<String, Object> createStorageOptionsMap(SqlHandlerConfig config, final SqlNodeList args) {
    if (args == null || args.size() == 0) {
      return null;
    }

    final ImmutableMap.Builder<String, Object> storageOptions = ImmutableMap.builder();
    for (SqlNode operand : args) {
      if (operand.getKind() != SqlKind.ARGUMENT_ASSIGNMENT) {
        throw UserException.unsupportedError()
            .message("Unsupported argument type. Only assignment arguments (param => value) are supported.")
            .build(logger);
      }
      final List<SqlNode> operandList = ((SqlCall) operand).getOperandList();

      final String name = ((SqlIdentifier) operandList.get(1)).getSimple();
      SqlNode literal = operandList.get(0);
      if (!(literal instanceof SqlLiteral)) {
        throw UserException.unsupportedError()
            .message("Only literals are accepted for storage option values")
            .build(logger);
      }

      Object value = ((SqlLiteral)literal).getValue();
      if (value instanceof NlsString) {
        value = ((NlsString)value).getValue();
      }
      storageOptions.put(name, value);
    }

    return storageOptions.build();
  }

  @Override
  public String getTextPlan() {
    return textPlan;
  }


}
