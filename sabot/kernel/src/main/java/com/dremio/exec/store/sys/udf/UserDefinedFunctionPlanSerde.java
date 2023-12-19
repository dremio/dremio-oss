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
package com.dremio.exec.store.sys.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.catalog.udf.DremioScalarUserDefinedFunction;
import com.dremio.exec.catalog.udf.DremioTabularUserDefinedFunction;
import com.dremio.exec.catalog.udf.FunctionParameterImpl;
import com.dremio.exec.ops.DelegatingPlannerCatalog;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.serialization.DeserializationException;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;

public final class UserDefinedFunctionPlanSerde {
  private static final Logger logger = LoggerFactory.getLogger(UserDefinedFunctionPlanSerde.class);

  private UserDefinedFunctionPlanSerde() {}

  public static RelNode deserialize(
    byte[] serializedPlan,
    RelOptCluster relOptCluster,
    QueryContext context,
    UserDefinedFunction udfForParameterResolution) throws DeserializationException {
    Pair<DremioCatalogReader, SqlOperatorTable> catalogReaderAndOperatorTable = extractCatalogAndOperatorTable(
      context,
      udfForParameterResolution);
    return RelSerializerFactory
      .getPlanningFactory(context.getConfig(), context.getScanResult())
      .getDeserializer(
        relOptCluster,
        catalogReaderAndOperatorTable.left,
        catalogReaderAndOperatorTable.right)
      .deserialize(serializedPlan);
  }

  public static Optional<RelNode> tryDeserialize(
    UserDefinedFunction udf,
    RelOptCluster relOptCluster,
    OptimizerRulesContext context) {
    if (udf.getSerializedFunctionPlan() == null) {
      return Optional.empty();
    }

    QueryContext queryContext;
    try {
      // This is the only implementation at the time.
      queryContext = (QueryContext) context;
    } catch (ClassCastException ex) {
      logger.info("Failed to cast OptimizerRulesContext to QueryContext with exception: " + ex);
      return Optional.empty();
    }

    try {
      RelNode deserializedPlan = UserDefinedFunctionPlanSerde.deserialize(
        udf.getSerializedFunctionPlan(),
        relOptCluster,
        queryContext,
        udf);
      return Optional.of(deserializedPlan);
    } catch (DeserializationException ex) {
      logger.info("Failed to deserialize udf: " + udf.getName() + " with exception: " + ex);
      return Optional.empty();
    }
  }

  public static byte[] serialize(
    RelNode deserializedPlan,
    QueryContext context,
    UserDefinedFunction udfForParameterResolution) {
    Pair<DremioCatalogReader,SqlOperatorTable> catalogReaderAndOperatorTable = extractCatalogAndOperatorTable(
      context,
      udfForParameterResolution);
    return RelSerializerFactory
      .getPlanningFactory(context.getConfig(), context.getScanResult())
      .getSerializer(deserializedPlan.getCluster(), catalogReaderAndOperatorTable.right)
      .serializeToBytes(deserializedPlan);
  }

  private static Pair<DremioCatalogReader, SqlOperatorTable> extractCatalogAndOperatorTable(
    QueryContext context,
    UserDefinedFunction udfForParameterResolution) {
    DremioCatalogReader dremioCatalogReader = new DremioCatalogReader(
      DelegatingPlannerCatalog.newInstance(context.getCatalog()));
    SqlOperatorTable parameterResolutionOperatorTable = new FunctionOperatorTable(
      udfForParameterResolution.getName(),
      FunctionParameterImpl.createParameters(udfForParameterResolution.getFunctionArgsList()));

    List<SqlOperator> existingUserDefinedFunctions = new ArrayList<>();
    try {
      for (UserDefinedFunction userDefinedFunction : context.getCatalog().getAllFunctions()) {
        SqlOperator sqlOperator = UserDefinedFunctionSqlOperator.create(userDefinedFunction);

        SqlUserDefinedFunction sqlUserDefinedFunction;
        if (userDefinedFunction.getReturnType().isStruct()) {
          sqlUserDefinedFunction = new SqlUserDefinedTableFunction(
            sqlOperator.getNameAsId(),
            sqlOperator.getReturnTypeInference(),
            sqlOperator.getOperandTypeInference(),
            sqlOperator.getOperandTypeChecker(),
            null,
            new DremioTabularUserDefinedFunction(null, userDefinedFunction));
        } else {
          sqlUserDefinedFunction = new SqlUserDefinedFunction(
            sqlOperator.getNameAsId(),
            sqlOperator.getReturnTypeInference(),
            sqlOperator.getOperandTypeInference(),
            sqlOperator.getOperandTypeChecker(),
            null,
            new DremioScalarUserDefinedFunction(null, userDefinedFunction)
          );
        }

        existingUserDefinedFunctions.add(sqlUserDefinedFunction);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    SqlOperatorTable userDefinedFunctionOperatorTable = new ListSqlOperatorTable(existingUserDefinedFunctions);

    SqlOperatorTable dremioCompositeSqlOperatorTable = DremioCompositeSqlOperatorTable.create(context.getFunctionRegistry());
    SqlOperatorTable dremioPlusUdfOperatorTable = ChainedSqlOperatorTable.of(
      userDefinedFunctionOperatorTable,
      parameterResolutionOperatorTable,
      dremioCompositeSqlOperatorTable);

    return Pair.of(dremioCatalogReader, dremioPlusUdfOperatorTable);
  }
}
