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
package com.dremio.exec.planner.sql.parser;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.jetbrains.annotations.Nullable;

/** Dremio dummy implementation of {@link SqlValidator} to adapt to upstream api changes. */
public class DremioSqlValidator implements SqlValidator {

  private final RelDataTypeFactory relDataTypeFactory;

  public DremioSqlValidator(RelDataTypeFactory relDataTypeFactory) {
    this.relDataTypeFactory = Objects.requireNonNull(relDataTypeFactory);
  }

  @Override
  public SqlConformance getConformance() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorCatalogReader getCatalogReader() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlOperatorTable getOperatorTable() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlNode validate(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlNode validateParameterizedExpression(SqlNode sqlNode, Map<String, RelDataType> map) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateQuery(
      SqlNode sqlNode, SqlValidatorScope sqlValidatorScope, RelDataType relDataType) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public RelDataType getValidatedNodeType(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public RelDataType getValidatedNodeTypeIfKnown(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateIdentifier(SqlIdentifier sqlIdentifier, SqlValidatorScope sqlValidatorScope) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateLiteral(SqlLiteral sqlLiteral) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateIntervalQualifier(SqlIntervalQualifier sqlIntervalQualifier) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateInsert(SqlInsert sqlInsert) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateUpdate(SqlUpdate sqlUpdate) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateDelete(SqlDelete sqlDelete) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateMerge(SqlMerge sqlMerge) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateDataType(SqlDataTypeSpec sqlDataTypeSpec) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateDynamicParam(SqlDynamicParam sqlDynamicParam) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateWindow(
      SqlNode sqlNode, SqlValidatorScope sqlValidatorScope, SqlCall sqlCall) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateMatchRecognize(SqlCall sqlCall) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateCall(SqlCall sqlCall, SqlValidatorScope sqlValidatorScope) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateAggregateParams(
      SqlCall sqlCall,
      SqlNode sqlNode,
      SqlNodeList sqlNodeList,
      SqlValidatorScope sqlValidatorScope) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateColumnListParams(
      SqlFunction sqlFunction, List<RelDataType> list, List<SqlNode> list1) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Nullable
  @Override
  public SqlCall makeNullaryCall(SqlIdentifier sqlIdentifier) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public RelDataType deriveType(SqlValidatorScope sqlValidatorScope, SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public CalciteContextException newValidationError(
      SqlNode sqlNode, ExInst<SqlValidatorException> exInst) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public boolean isAggregate(SqlSelect sqlSelect) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public boolean isAggregate(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlWindow resolveWindow(SqlNode sqlNode, SqlValidatorScope sqlValidatorScope, boolean b) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorNamespace getNamespace(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public String deriveAlias(SqlNode sqlNode, int i) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlNodeList expandStar(SqlNodeList sqlNodeList, SqlSelect sqlSelect, boolean b) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getWhereScope(SqlSelect sqlSelect) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public RelDataTypeFactory getTypeFactory() {
    return this.relDataTypeFactory;
  }

  @Override
  public void setValidatedNodeType(SqlNode sqlNode, RelDataType relDataType) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void removeValidatedNodeType(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public RelDataType getUnknownType() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getSelectScope(SqlSelect sqlSelect) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SelectScope getRawSelectScope(SqlSelect sqlSelect) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getFromScope(SqlSelect sqlSelect) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getJoinScope(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getGroupScope(SqlSelect sqlSelect) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getHavingScope(SqlSelect sqlSelect) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getOrderScope(SqlSelect sqlSelect) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getLateralScope(SqlJoin sqlJoin) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getMatchRecognizeScope(SqlMatchRecognize sqlMatchRecognize) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void declareCursor(SqlSelect sqlSelect, SqlValidatorScope sqlValidatorScope) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void pushFunctionCall() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void popFunctionCall() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public String getParentCursor(String s) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void setIdentifierExpansion(boolean b) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void setColumnReferenceExpansion(boolean b) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public boolean getColumnReferenceExpansion() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void setDefaultNullCollation(NullCollation nullCollation) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public NullCollation getDefaultNullCollation() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public boolean shouldExpandIdentifiers() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void setCallRewrite(boolean b) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public RelDataType deriveConstructorType(
      SqlValidatorScope sqlValidatorScope,
      SqlCall sqlCall,
      SqlFunction sqlFunction,
      SqlFunction sqlFunction1,
      List<RelDataType> list) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public CalciteException handleUnresolvedFunction(
      SqlCall sqlCall, SqlFunction sqlFunction, List<RelDataType> list, List<String> list1) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlNode expandOrderExpr(SqlSelect sqlSelect, SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlNode expand(SqlNode sqlNode, SqlValidatorScope sqlValidatorScope) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public boolean isSystemField(RelDataTypeField relDataTypeField) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public List<List<String>> getFieldOrigins(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public RelDataType getParameterRowType(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getOverScope(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public boolean validateModality(SqlSelect sqlSelect, SqlModality sqlModality, boolean b) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateWith(SqlWith sqlWith, SqlValidatorScope sqlValidatorScope) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateWithItem(SqlWithItem sqlWithItem) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void validateSequenceValue(
      SqlValidatorScope sqlValidatorScope, SqlIdentifier sqlIdentifier) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidatorScope getWithScope(SqlNode sqlNode) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidator setEnableTypeCoercion(boolean b) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public boolean isTypeCoercionEnabled() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void setTypeCoercion(TypeCoercion typeCoercion) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public TypeCoercion getTypeCoercion() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public boolean hasValidationMessage() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public String getValidationMessage() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public void setValidationMessage(String s) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public Config config() {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }

  @Override
  public SqlValidator transform(UnaryOperator<Config> unaryOperator) {
    throw new RuntimeException(
        "DremioSqlValidator is a dummy SqlValidator to adapt to upstream api changes.");
  }
}
