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
package com.dremio.exec.planner.physical;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.physical.visitor.InsertLocalExchangeVisitor.RexNodeBasedHashExpressionCreatorHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Contains utility methods for creating hash expression for either distribution (in PartitionSender) or for HashTable.
 */
public class HashPrelUtil {

  public static final String HASH_EXPR_NAME = "E_X_P_R_H_A_S_H_F_I_E_L_D";

  /**
   * Interface for creating different forms of hash expression types.
   * @param <T>
   */
  public interface HashExpressionCreatorHelper<T> {
    T createCall(String funcName, List<T> inputFiled);
  }

  /**
   * Implementation of {@link HashExpressionCreatorHelper} for {@link LogicalExpression} type.
   */
  public static HashExpressionCreatorHelper<LogicalExpression> HASH_HELPER_LOGICALEXPRESSION =
      new HashExpressionCreatorHelper<LogicalExpression>() {
        @Override
        public LogicalExpression createCall(String funcName, List<LogicalExpression> inputFiled) {
          return new FunctionCall(funcName, inputFiled);
        }
      };

  // The hash32 functions actually use hash64 underneath.  The reason we want to call hash32 is that
  // the hash based operators make use of 4 bytes of hash value, not 8 bytes (for reduced memory use).
  public static final String HASH32_FUNCTION_NAME = "hash32";
  public static final String DREMIO_SPLIT_DISTRIBUTE_HASH_FUNCTION_NAME = "dremioSplitDistribute";
  private static final String HASH32_DOUBLE_FUNCTION_NAME = "hash32AsDouble";

  /**
   * Create hash based partition expression based on the given distribution fields.
   *
   * @param distFields Field list based on which the distribution partition expression is constructed.
   * @param helper Implementation of {@link HashExpressionCreatorHelper}
   *               which is used to create function expressions.
   * @param <T> Input and output expression type.
   *           Currently it could be either {@link RexNode} or {@link LogicalExpression}
   * @return
   */
  public static <T> T createHashBasedPartitionExpression(
      List<T> distFields,
      HashExpressionCreatorHelper<T> helper) {
    return createHashExpression(distFields, helper);
  }

  public static <T> T createHashBasedPartitionExpression(
      List<T> distFields,
      HashExpressionCreatorHelper<T> helper,
      final String functionName) {
    return createHashExpression(distFields, helper, functionName);
  }

  /**
   * Create hash expression based on the given input fields.
   *
   * @param inputExprs Expression list based on which the hash expression is constructed.
   * @param helper Implementation of {@link HashExpressionCreatorHelper}
   *               which is used to create function expressions.
   * @param <T> Input and output expression type.
   *           Currently it could be either {@link RexNode} or {@link LogicalExpression}
   * @return
   */
  public static <T> T createHashExpression(
          List<T> inputExprs,
          HashExpressionCreatorHelper<T> helper) {
    return createHashExpression(inputExprs, helper, HASH32_FUNCTION_NAME);
  }

  public static <T> T createHashExpression(
      List<T> inputExprs,
      HashExpressionCreatorHelper<T> helper,
      final String functionName) {

    assert inputExprs.size() > 0;

    T func = helper.createCall(functionName,  ImmutableList.of(inputExprs.get(0)));
    for (int i = 1; i<inputExprs.size(); i++) {
      func = helper.createCall(functionName, ImmutableList.of(inputExprs.get(i), func));
    }

    return func;
  }

  /**
   * Return a hash expression :  hash32(field1, hash32(field2, hash32(field3, 0)));
   */
  public static LogicalExpression getHashExpression(List<LogicalExpression> fields){
    return createHashExpression(fields, HASH_HELPER_LOGICALEXPRESSION);
  }

  public static ProjectPrel addHashProject(List<DistributionField> distFields, Prel input, Integer ringCount) {
    return addHashProject(distFields, input, ringCount, HASH32_FUNCTION_NAME);
  }

  public static ProjectPrel addHashProject(List<DistributionField> distFields, Prel input, Integer ringCount, String hashFunctionName){

    // Insert Project SqlOperatorImpl with new column that will be a hash for HashToRandomExchange fields

    final List<String> outputFieldNames = Lists.newArrayList(input.getRowType().getFieldNames());
    final String fieldName = ringCount == null ? HashPrelUtil.HASH_EXPR_NAME : WriterPrel.BUCKET_NUMBER_FIELD;
    outputFieldNames.add(fieldName);

    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    final List<RelDataTypeField> childRowTypeFields = input.getRowType().getFieldList();

    // create new hashed field.
    final HashExpressionCreatorHelper<RexNode> hashHelper = new RexNodeBasedHashExpressionCreatorHelper(rexBuilder);
    final List<RexNode> distFieldRefs = Lists.newArrayListWithExpectedSize(distFields.size());
    for(int i = 0; i < distFields.size(); i++) {
      final int fieldId = distFields.get(i).getFieldId();
      distFieldRefs.add(rexBuilder.makeInputRef(childRowTypeFields.get(fieldId).getType(), fieldId));
    }

    final List <RexNode> updatedExpr = Lists.newArrayListWithExpectedSize(childRowTypeFields.size() + 1);
    for ( RelDataTypeField field : childRowTypeFields) {
      RexNode rex = rexBuilder.makeInputRef(field.getType(), field.getIndex());
      updatedExpr.add(rex);
    }
    RexNode hashExpression = HashPrelUtil.createHashBasedPartitionExpression(distFieldRefs, hashHelper, hashFunctionName);

    if(ringCount != null){
      RelDataType intType = input.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER);
      hashExpression = rexBuilder.makeCall(SqlStdOperatorTable.MOD, ImmutableList.of(hashExpression, rexBuilder.makeExactLiteral(BigDecimal.valueOf(ringCount), intType)));
      hashExpression = rexBuilder.makeCall(SqlStdOperatorTable.ABS, Collections.singletonList(hashExpression));
    }
    updatedExpr.add(hashExpression);

    RelDataType rowType = RexUtil.createStructType(input.getCluster().getTypeFactory(), updatedExpr, outputFieldNames);

    ProjectPrel addColumnprojectPrel = ProjectPrel.create(input.getCluster(), input.getTraitSet(), input, updatedExpr, rowType);
    return addColumnprojectPrel;
  }

  /**
   * Create a distribution hash expression.
   *
   * @param fields Distribution fields
   * @param rowType Row type
   * @return
   */
  public static LogicalExpression getHashExpression(List<DistributionField> fields, RelDataType rowType) {
    assert fields.size() > 0;

    final List<String> childFields = rowType.getFieldNames();

    // If we already included a field with hash - no need to calculate hash further down
    if ( childFields.contains(HASH_EXPR_NAME)) {
      return new FieldReference(HASH_EXPR_NAME);
    }

    final List<LogicalExpression> expressions = new ArrayList<>(childFields.size());
    for(int i =0; i < fields.size(); i++){
      expressions.add(new FieldReference(childFields.get(fields.get(i).getFieldId())));
    }

    return createHashBasedPartitionExpression(expressions, HASH_HELPER_LOGICALEXPRESSION);
  }
}
