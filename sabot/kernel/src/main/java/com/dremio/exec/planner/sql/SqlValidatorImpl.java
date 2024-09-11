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
package com.dremio.exec.planner.sql;

import static com.dremio.exec.ExecConstants.SHOW_CREATE_ENABLED;
import static com.dremio.exec.tablefunctions.DremioCalciteResource.DREMIO_CALCITE_RESOURCE;
import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.util.Static.RESOURCE;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.physical.DmlPositionalMergeOnReadPlanGenerator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.DmlUtils;
import com.dremio.exec.planner.sql.parser.SqlCopyIntoTable;
import com.dremio.exec.planner.sql.parser.SqlDeleteFromTable;
import com.dremio.exec.planner.sql.parser.SqlDmlOperator;
import com.dremio.exec.planner.sql.parser.SqlInsertTable;
import com.dremio.exec.planner.sql.parser.SqlMergeIntoTable;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.planner.sql.parser.SqlShowCreate;
import com.dremio.exec.planner.sql.parser.SqlUpdateTable;
import com.dremio.exec.planner.sql.parser.SqlVersionedTableCollectionCall;
import com.dremio.exec.planner.sql.parser.SqlVersionedTableMacroCall;
import com.dremio.exec.tablefunctions.TableMacroNames;
import com.dremio.exec.tablefunctions.VersionedTableMacro;
import com.dremio.options.OptionResolver;
import com.dremio.options.TypeValidators;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction;
import org.apache.calcite.sql.fun.SqlNtileAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.DremioEmptyScope;
import org.apache.calcite.sql.validate.DremioParameterScope;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.ProcedureNamespace;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlScopedShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.apache.iceberg.RowLevelOperationMode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlValidatorImpl extends org.apache.calcite.sql.validate.SqlValidatorImpl {
  private final FlattenOpCounter flattenCount;
  private final OptionResolver optionResolver;

  public SqlValidatorImpl(
      FlattenOpCounter flattenCount,
      SqlOperatorTable sqlOperatorTable,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      Config config,
      OptionResolver optionResolver) {
    super(sqlOperatorTable, catalogReader, typeFactory, config);
    this.flattenCount = flattenCount;
    this.optionResolver = optionResolver;
  }

  @Override
  public SqlNode validate(SqlNode topNode) {
    checkForFeatureSpecificSyntax(topNode, optionResolver);
    final SqlValidatorScope scope = createBaseScope(topNode);
    final SqlNode topNode2 = validateScopedExpression(topNode, scope);
    final RelDataType type = getValidatedNodeType(topNode2);
    Util.discard(type);
    return topNode2;
  }

  @Override
  protected SqlNode performUnconditionalRewrites(SqlNode node, boolean underFrom) {
    if (node instanceof SqlBasicCall
        && ((SqlBasicCall) node).getOperator() instanceof SqlJdbcFunctionCall) {
      // Check for operator overrides in DremioSqlOperatorTable
      SqlBasicCall call = (SqlBasicCall) node;
      final SqlJdbcFunctionCall function = (SqlJdbcFunctionCall) call.getOperator();
      final List<SqlOperator> overloads = new ArrayList<>();
      // The name is in the format {fn operator_name}, so we need to remove the prefix '{fn ' and
      // the suffix '}' to get the original operators name.
      String functionName = function.getName().substring(4, function.getName().length() - 1);
      // ROUND and TRUNCATE have been overridden in DremioSqlOperatorTable
      if (functionName.equalsIgnoreCase(DremioSqlOperatorTable.ROUND.getName())) {
        call.setOperator(DremioSqlOperatorTable.ROUND);
      } else if (functionName.equalsIgnoreCase(DremioSqlOperatorTable.TRUNCATE.getName())) {
        call.setOperator(DremioSqlOperatorTable.TRUNCATE);
      }
    } else if (node instanceof SqlMergeIntoTable) {
      SqlMergeIntoTable merge = (SqlMergeIntoTable) node;
      rewriteMerge(merge);
      return node;
    } else if (node instanceof SqlOptimize) {
      SqlOptimize sqlOptimize = (SqlOptimize) node;
      SqlSelect select = createSourceSelectForOptimize(sqlOptimize);
      sqlOptimize.setSourceSelect(select);
      return node;
    } else if (node instanceof SqlCopyIntoTable) {
      SqlCopyIntoTable sqlCopyIntoTable = (SqlCopyIntoTable) node;
      SqlSelect select = createSourceSelectForCopyIntoTable(sqlCopyIntoTable);
      sqlCopyIntoTable.setSourceSelect(select);
      return node;
    } else if (node instanceof SqlJoin && ((SqlJoin) node).getCondition() instanceof SqlBasicCall) {
      node = rewriteJoinWithVersionedTables(node);
    }

    return super.performUnconditionalRewrites(node, underFrom);
  }

  private SqlNode rewriteJoinWithVersionedTables(SqlNode node) {
    SqlJoin sqlJoin = (SqlJoin) node;
    SqlBasicCall sqlCallOriginalCondition = (SqlBasicCall) sqlJoin.getCondition();
    SqlNode[] joinNodes = new SqlNode[] {sqlJoin.getLeft(), sqlJoin.getRight()};
    SqlNode[] conditionNodes = sqlCallOriginalCondition.getOperands();
    boolean isVersionedTableInCondition = false;

    // Extract the TableNames of the JoinNodes which is of SqlVersionedTableCollectionCall (means
    // having AT syntax)
    Set<String> tableNames = new HashSet<>();
    for (SqlNode joinNode : joinNodes) {
      if (joinNode instanceof SqlVersionedTableCollectionCall) {
        SqlVersionedTableCollectionCall versionedTableCollectionCall =
            (SqlVersionedTableCollectionCall) joinNode;
        for (SqlNode tableOperand : versionedTableCollectionCall.getOperandList()) {
          if (tableOperand instanceof SqlVersionedTableMacroCall) {
            SqlVersionedTableMacroCall versionedTableMacroCall =
                (SqlVersionedTableMacroCall) tableOperand;
            String tableName = getTableNameFromVersionedTableCall(null, versionedTableMacroCall);
            tableNames.add(tableName);
          }
        }
      }
    }

    // If none of the tables (referred in SqlVersionedTableCollectionCall) are captured then just
    // return the node
    if (tableNames.isEmpty()) {
      return node;
    }

    // Loop over both the conditions of Join node
    for (int i = 0; i < conditionNodes.length; i++) {
      // Only modify the SqlNode condition's FQID to only contain the table name if the condition
      // node is of SqlVersionedTableCollectionCall
      if (conditionNodes[i] instanceof SqlIdentifier) {
        SqlIdentifier sqlIdentifier = (SqlIdentifier) conditionNodes[i];
        // Since we already extracted the versioned table names previously, so we can just check if
        // any of the condition node matches with the table names set
        if (sqlIdentifier.names.size() > 2
            && tableNames.contains(sqlIdentifier.names.get(sqlIdentifier.names.size() - 2))) {
          conditionNodes[i] =
              sqlIdentifier.getComponent(
                  sqlIdentifier.names.size() - 2, sqlIdentifier.names.size());
          isVersionedTableInCondition = true;
        }
      }
    }

    if (isVersionedTableInCondition) {
      SqlNode sqlNodeCondition =
          new SqlBasicCall(
              sqlCallOriginalCondition.getOperator(),
              conditionNodes,
              sqlCallOriginalCondition.getParserPosition());
      node =
          new SqlJoin(
              sqlJoin.getParserPosition(),
              sqlJoin.getLeft(),
              sqlJoin.isNaturalNode(),
              sqlJoin.getJoinTypeNode(),
              sqlJoin.getRight(),
              sqlJoin.getConditionTypeNode(),
              sqlNodeCondition);
    }

    return node;
  }

  private SqlSelect createSourceSelectForOptimize(SqlOptimize call) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    SqlNode sourceTable = call.getTable();
    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        sourceTable,
        call.getCondition(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private SqlSelect createSourceSelectForCopyIntoTable(SqlCopyIntoTable call) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    List<SqlNode> selectNodes = call.getSelectNodes();
    if (selectNodes.isEmpty()) {
      selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    } else {
      selectNodes.forEach(selectList::add);
    }
    SqlNode table = call.getTargetTable();

    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        table,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  @Override
  protected void registerNamespace(
      SqlValidatorScope usingScope, String alias, SqlValidatorNamespace ns, boolean forceNullable) {

    // Update aliases for __system_table_macros.time_travel calls so that table aliases behave
    // similarly when they have
    // a version clause.  The default should be the table name, not EXPR$N.
    if (ns instanceof ProcedureNamespace) {
      if (ns.getNode() instanceof SqlVersionedTableMacroCall
          && ns.getEnclosingNode().getKind() != SqlKind.AS) {
        SqlVersionedTableMacroCall call = (SqlVersionedTableMacroCall) ns.getNode();
        alias = getTableNameFromVersionedTableCall(alias, call);
      }
    }

    super.registerNamespace(usingScope, alias, ns, forceNullable);
  }

  private static String getTableNameFromVersionedTableCall(
      String alias, SqlVersionedTableMacroCall call) {
    if (call.getOperator().getNameAsId().names.equals(TableMacroNames.TIME_TRAVEL)
        && call.getOperands().length == 1
        && call.getOperands()[0] instanceof SqlLiteral) {
      // extract the table name from the full qualified name passed as an arg to the table macro
      SqlLiteral qualifiedName = (SqlLiteral) call.getOperands()[0];
      List<String> nameParts =
          VersionedTableMacro.splitTableIdentifier(qualifiedName.getValueAs(String.class));
      if (!nameParts.isEmpty()) {
        alias = nameParts.get(nameParts.size() - 1);
      }
    }
    return alias;
  }

  @Override
  public SqlNode validateParameterizedExpression(
      SqlNode topNode, final Map<String, RelDataType> nameToTypeMap) {
    SqlValidatorScope scope = new DremioParameterScope(this, nameToTypeMap);
    return validateScopedExpression(topNode, scope);
  }

  @Override
  public SqlValidatorNamespace getNamespace(SqlNode node) {
    // Add Extend to super's fall-through cases
    if (node.getKind() == SqlKind.EXTEND) {
      return getNamespace(((SqlCall) node).operand(0));
    }
    return super.getNamespace(node);
  }

  @Override
  public void validateJoin(SqlJoin join, SqlValidatorScope scope) {
    SqlNode condition = join.getCondition();
    checkIfFlattenIsPartOfJoinCondition(condition);
    super.validateJoin(join, scope);
  }

  private void checkIfFlattenIsPartOfJoinCondition(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) node;
      SqlNode[] conditionOperands = call.getOperands();
      for (SqlNode operand : conditionOperands) {
        if (operand instanceof SqlBasicCall) {
          if (((SqlBasicCall) operand).getOperator().getName().equalsIgnoreCase("flatten")) {
            throwException(node.getParserPosition());
          }
        }
        checkIfFlattenIsPartOfJoinCondition(operand);
      }
    }
  }

  private void throwException(SqlParserPos parserPos) {
    throw new CalciteContextException(
        "Failure parsing the query",
        new SqlValidatorException("Flatten is not supported as part of join condition", null),
        parserPos.getLineNum(),
        parserPos.getEndLineNum(),
        parserPos.getColumnNum(),
        parserPos.getEndColumnNum());
  }

  int nextFlattenIndex() {
    return flattenCount.nextFlattenIndex();
  }

  public static class FlattenOpCounter {
    private int value;

    int nextFlattenIndex() {
      return value++;
    }
  }

  private SqlValidatorScope createBaseScope(SqlNode sqlNode) {
    return DremioEmptyScope.createBaseScope(this);
  }

  private void checkFieldCount(
      SqlNode node, RelDataType logicalSourceRowType, RelDataType logicalTargetRowType) {
    final int sourceFieldCount = logicalSourceRowType.getFieldCount();
    final int targetFieldCount = logicalTargetRowType.getFieldCount();
    if (sourceFieldCount != targetFieldCount) {
      throw newValidationError(
          node, DREMIO_CALCITE_RESOURCE.unmatchColumn(targetFieldCount, sourceFieldCount));
    }
  }

  /**
   * Based on Calcite's validateUpdate: Skip following validations 1. constraint check 2. Access
   * Control check (we have our own AC control).
   *
   * <p>Add "condition" validation
   */
  @Override
  public void validateUpdate(SqlUpdate call) {
    Preconditions.checkState(
        call instanceof SqlUpdateTable, "only SqlUpdateTable is expected here");

    final SqlValidatorNamespace targetNamespace = getNamespace(call);
    validateNamespace(targetNamespace, unknownType);
    final RelOptTable relOptTable =
        SqlValidatorUtil.getRelOptTable(
            targetNamespace, getCatalogReader().unwrap(Prepare.CatalogReader.class), null, null);
    final SqlValidatorTable table =
        relOptTable == null
            ? targetNamespace.getTable()
            : relOptTable.unwrap(SqlValidatorTable.class);
    RelDataType targetRowType = createTargetRowType(table, call.getTargetColumnList(), true);

    final SqlSelect select = call.getSourceSelect();
    validateSelect(select, targetRowType);

    SqlNode sourceNode =
        ((SqlUpdateTable) call).getSourceTableRef() == null ? call : call.getSourceSelect();
    final RelDataType sourceRowType = getNamespace(sourceNode).getRowType();

    // Handling Update * in Merge query
    if (isSqlDmlOperatorWithStar(call)) {
      // set sourceExpressionList
      call.setOperand(2, select.getSelectList());

      // set targetColumnList
      SqlNodeList targetColumnList =
          new SqlNodeList(
              table.getRowType().getFieldNames().stream()
                  .map(field -> new SqlIdentifier(field, SqlParserPos.ZERO))
                  .collect(Collectors.toList()),
              SqlParserPos.ZERO);
      call.setOperand(1, targetColumnList);

      // validate the field count match
      checkFieldCount(call.getTargetTable(), sourceRowType, targetRowType);
    }

    // validate "condition".
    // "condition" is one of the operators of SqlUpdate. replaceSubQueries in convertUpdate needs
    // check all operators for In clause.
    // We need validate "condition" and register it to namespace.
    if (call.getCondition() != null) {
      validateScopedExpression(call.getCondition(), getWhereScope(select));
    }

    if (((SqlUpdateTable) call).getSourceTableRef() == null) {
      checkTypeAssignment(sourceRowType, targetRowType, call);
    } else {
      checkTypeAssignmentForUpdateWithSource(sourceRowType, targetRowType, call);
    }
  }

  /**
   * The layout of targetRowType is: original target table columns + dml system columns (filePath,
   * rowIndex) + target columns The layout of sourceRowType for Update query with source is: dml
   * system columns (filePath, rowIndex) + target columns This function is to compare the type
   * assignment for target columns only, in a backwards direction
   */
  private void checkTypeAssignmentForUpdateWithSource(
      RelDataType sourceRowType, RelDataType targetRowType, final SqlNode query) {
    Preconditions.checkArgument(query instanceof SqlUpdate);

    List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
    List<RelDataTypeField> targetFields = targetRowType.getFieldList();

    final int targetColumnCount = ((SqlUpdate) query).getTargetColumnList().size();
    checkTypeAssignment(sourceFields, targetFields, targetColumnCount, query);
  }

  private void checkTypeAssignment(
      List<RelDataTypeField> sourceFields,
      List<RelDataTypeField> targetFields,
      int columnsToCompare,
      final SqlNode query) {

    final int sourceCount = sourceFields.size();
    final int targetCount = targetFields.size();

    // compare target columns between source table and target table, in a backwards direction
    for (int i = 0; i < columnsToCompare; ++i) {
      RelDataTypeField sourceField = sourceFields.get(sourceCount - i - 1);
      RelDataTypeField targetField = targetFields.get(targetCount - i - 1);
      RelDataType sourceType = sourceField.getType();
      RelDataType targetType = targetField.getType();
      if (!MoreRelOptUtil.checkFieldTypesCompatibility(sourceType, targetType, false, false)) {
        String targetTypeString;
        String sourceTypeString;
        if (SqlTypeUtil.areCharacterSetsMismatched(sourceType, targetType)) {
          sourceTypeString = sourceType.getFullTypeString();
          targetTypeString = targetType.getFullTypeString();
        } else {
          sourceTypeString = sourceType.toString();
          targetTypeString = targetType.toString();
        }
        throw newValidationError(
            query,
            RESOURCE.typeNotAssignable(
                targetField.getName(), targetTypeString,
                sourceField.getName(), sourceTypeString));
      }
    }
  }

  /**
   * Simplified version of Calcite's validateMerge: Skip some unecessary validation For example,
   * Access Control check (we have our own AC control). Calcite's validateMerge uses either
   * updateCall's or insertCall's targetRowType as sourceSelect's targetRowType, which does not work
   * with Dremio's extended tables. User unknownType here instead
   *
   * <p>validation list: 1. source select 2. update call 3. insert call
   */
  @Override
  public void validateMerge(SqlMerge call) {
    // Apply customized validate to Dremio's SqlMergeTable only
    if (!(call instanceof SqlMergeIntoTable)) {
      super.validate(call);
      return;
    }

    IdentifierNamespace targetNamespace = (IdentifierNamespace) getNamespace(call.getTargetTable());
    validateNamespace(targetNamespace, unknownType);

    SqlSelect sqlSelect = call.getSourceSelect();
    validateSelect(sqlSelect, unknownType);

    SqlUpdate updateCall = call.getUpdateCall();
    if (updateCall != null) {
      validateUpdate(updateCall);

      if (isSqlDmlOperatorWithStar(updateCall)) {
        // Above validateUpdate will expand * into concrete select items in Update * clause.
        // Those concrete select items need to be added to the end of Merge call's select list
        updateCall.getSourceSelect().getSelectList().getList().stream()
            .forEach(s -> sqlSelect.getSelectList().add(s));
      }
    }
    SqlInsertTable insertCall = (SqlInsertTable) call.getInsertCall();
    if (insertCall != null) {
      if (insertCall.getQuery() instanceof SqlIdentifier
          && ((SqlIdentifier) (insertCall.getQuery())).isStar()) {
        validateMergeInsertWithStar(insertCall);
      } else {
        validateInsert(insertCall);
      }
    }
  }

  /**
   * The source of Merge's insert is set as the join between source table and target table in
   * rewriteMerge() The selectList of the joined table is * for insert clause "Insert *" Since we
   * dont have the specific column list in the select list, we are going to validate the whole
   * joined table as the insert source. This function is customized to validate the source table
   * portion of the joined table 1. check the length of source table is the same as target table 2.
   * check the source column assignable to target columns
   */
  private void validateMergeInsertWithStar(SqlInsert insert) {
    Preconditions.checkArgument(insert instanceof SqlInsertTable);

    SqlValidatorNamespace targetNamespace = this.getNamespace(insert);
    this.validateNamespace(targetNamespace, this.unknownType);
    RelOptTable relOptTable =
        SqlValidatorUtil.getRelOptTable(
            targetNamespace, getCatalogReader().unwrap(Prepare.CatalogReader.class), null, null);
    SqlValidatorTable table =
        relOptTable == null
            ? targetNamespace.getTable()
            : relOptTable.unwrap(SqlValidatorTable.class);
    RelDataType targetRowType =
        this.createTargetRowType(table, insert.getTargetColumnList(), false);
    SqlNode source = insert.getSource();
    if (source instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) source;
      this.validateSelect(sqlSelect, targetRowType);
    } else {
      SqlValidatorScope scope = this.scopes.get(source);
      this.validateQuery(source, scope, targetRowType);
    }

    RelDataType sourceRowType = this.getNamespace(source).getRowType();
    RelDataType logicalTargetRowType = this.getLogicalTargetRowType(targetRowType, insert);
    this.setValidatedNodeType(insert, logicalTargetRowType);
    RelDataType logicalSourceRowType = this.getLogicalSourceRowType(sourceRowType, insert);

    // source of the Insert in the merge query is a join between the source table and target table
    // source table column count = joined table column count - target table columns - system column
    // count
    int sourceFieldCountFromMergeJoin = logicalSourceRowType.getFieldCount();
    int targetFieldCount = logicalTargetRowType.getFieldCount();
    int sourceFieldCount =
        sourceFieldCountFromMergeJoin - targetFieldCount - DmlUtils.SYSTEM_COLUMN_COUNT;
    if (sourceFieldCount != targetFieldCount) {
      throw this.newValidationError(
          insert, Static.RESOURCE.unmatchInsertColumn(targetFieldCount, sourceFieldCount));
    }
    checkTypeAssignmentForMergeInsertWithStar(logicalSourceRowType, targetRowType, insert);
  }

  private void checkTypeAssignmentForMergeInsertWithStar(
      RelDataType sourceRowType, RelDataType targetRowType, final SqlInsert insert) {

    List<RelDataTypeField> targetFields = targetRowType.getFieldList();
    List<RelDataTypeField> sourceFields =
        sourceRowType.getFieldList().subList(0, targetFields.size());

    checkTypeAssignment(sourceFields, targetFields, targetFields.size(), insert);
  }

  @Override
  public void validateWindow(SqlNode windowOrId, SqlValidatorScope scope, SqlCall call) {
    super.validateWindow(windowOrId, scope, call);
    final SqlWindow targetWindow;
    switch (windowOrId.getKind()) {
      case IDENTIFIER:
        targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
        break;
      case WINDOW:
        targetWindow = (SqlWindow) windowOrId;
        break;
      default:
        return;
    }

    SqlNodeList orderList = targetWindow.getOrderList();
    SqlOperator operator = call.getOperator();
    Exception e = null;
    if (operator instanceof SqlLeadLagAggFunction || operator instanceof SqlNtileAggFunction) {
      if (orderList.size() == 0) {
        e =
            new SqlValidatorException(
                "LAG, LEAD or NTILE functions require ORDER BY clause in window specification",
                null);
      }
    }

    if (orderList.getList().stream().anyMatch(f -> f instanceof SqlNumericLiteral)) {
      e =
          new SqlValidatorException(
              "Dremio does not currently support order by with ordinals in over clause", null);
    }

    if (e != null) {
      SqlParserPos pos = targetWindow.getParserPosition();
      CalciteContextException ex =
          RESOURCE.validatorContextPoint(pos.getLineNum(), pos.getColumnNum()).ex(e);
      ex.setPosition(pos.getLineNum(), pos.getColumnNum());
      throw ex;
    }
  }

  @Override
  public void validateAggregateParams(
      SqlCall aggCall, SqlNode filter, SqlNodeList orderList, SqlValidatorScope scope) {
    super.validateAggregateParams(aggCall, filter, orderList, scope);
  }

  @Override
  public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
    Preconditions.checkNotNull(scope);
    Expander expander = new Expander(this, scope);
    SqlNode newExpr = expr.accept(expander);
    if (expr != newExpr) {
      this.setOriginal(newExpr, expr);
    }

    return newExpr;
  }

  @Override
  public SqlNode expandWithAlias(SqlNode expr, SqlValidatorScope scope, SqlSelect select) {
    Preconditions.checkNotNull(scope);

    if ((optionResolver == null) || !optionResolver.getOption(PlannerSettings.EXTENDED_ALIAS)) {
      return super.expand(expr, scope);
    }

    final Expander expander = new ExtendedAliasExpander(this, scope, select, false);
    SqlNode newExpr = expr.accept(expander);
    if (expr != newExpr) {
      setOriginal(newExpr, expr);
    }

    return newExpr;
  }

  @Override
  public SqlNode expandWhereWithAlias(SqlNode expr, SqlValidatorScope scope, SqlSelect select) {
    if ((optionResolver == null) || !optionResolver.getOption(PlannerSettings.EXTENDED_ALIAS)) {
      return super.expand(expr, scope);
    }

    final Expander expander = new ExtendedAliasExpander(this, scope, select, true);
    SqlNode newExpr = expr.accept(expander);
    if (expr != newExpr) {
      setOriginal(newExpr, expr);
    }

    return newExpr;
  }

  /** Overriden to Handle the ITEM operator. */
  @Override
  protected @Nullable SqlNode stripDot(@Nullable SqlNode node) {
    // Checking for Item operator which is similiar to the dot operator.
    if (null == node) {
      return null;
    } else if (node.getKind() == SqlKind.DOT) {
      return stripDot(((SqlCall) node).operand(0));
    } else if (node.getKind() == SqlKind.OTHER_FUNCTION
        && SqlStdOperatorTable.ITEM == ((SqlCall) node).getOperator()) {
      return stripDot(((SqlCall) node).operand(0));
    } else {
      return node;
    }
  }

  /** We are seeing nested AS nodes that reference SqlIdentifiers. */
  @Override
  protected void checkRollUp(
      SqlNode grandParent,
      SqlNode parent,
      SqlNode current,
      SqlValidatorScope scope,
      String optionalClause) {
    current = stripAs(current);
    if (current instanceof SqlCall && !(current instanceof SqlSelect)) {
      // Validate OVER separately
      checkRollUpInWindow(getWindowInOver(current), scope);
      current = stripOver(current);

      SqlNode stripped = stripAs(stripDot(current));

      if (stripped instanceof SqlCall) {
        List<SqlNode> children = ((SqlCall) stripped).getOperandList();
        for (SqlNode child : children) {
          checkRollUp(parent, current, child, scope, optionalClause);
        }
      } else {
        current = stripped;
      }
    }
    if (current instanceof SqlIdentifier) {
      SqlIdentifier id = (SqlIdentifier) current;
      if (!id.isStar() && isRolledUpColumn(id, scope)) {
        if (!isAggregation(parent.getKind())
            || !isRolledUpColumnAllowedInAgg(id, scope, (SqlCall) parent, grandParent)) {
          String context = optionalClause != null ? optionalClause : parent.getKind().toString();
          throw newValidationError(id, RESOURCE.rolledUpNotAllowed(deriveAlias(id, 0), context));
        }
      }
    }
  }

  private static SqlNodeList getExtendedColumns(SqlNode node) {
    Preconditions.checkState(SqlDmlOperator.class.isAssignableFrom(node.getClass()));

    SqlDmlOperator sqlDmlOperator = (SqlDmlOperator) node;
    if (!sqlDmlOperator.isTableExtended()) {
      return null;
    }

    SqlBasicCall extendCall = (SqlBasicCall) sqlDmlOperator.getTargetTable();
    SqlNode[] operands = extendCall.getOperands();
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    SqlNodeList extendedColumns = (SqlNodeList) operands[1];
    // extract column identity only
    for (int i = 0; i < extendedColumns.size(); i = i + 2) {
      selectList.add(extendedColumns.get(i));
    }

    return selectList;
  }

  private SqlNode joinSourceAndTargetTable(
      SqlNode targetTable, SqlNode sourceTable, SqlNode condition, JoinType joinType) {

    final SqlNode sourceTableCopy = DeepCopier.copy(sourceTable);

    return new SqlJoin(
        SqlParserPos.ZERO,
        sourceTableCopy,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        joinType.symbol(SqlParserPos.ZERO),
        targetTable,
        condition == null
            ? JoinConditionType.NONE.symbol(SqlParserPos.ZERO)
            : JoinConditionType.ON.symbol(SqlParserPos.ZERO),
        condition);
  }

  /** Create selects by joining target table with source table */
  private SqlSelect createSourceSelectForSqlDmlOperator(SqlCall call) {
    Preconditions.checkState(call instanceof SqlDmlOperator);
    SqlNodeList selectList = getExtendedColumns(call);
    if (selectList == null) {
      return null;
    }

    SqlDmlOperator sqlDmlOperatorCall = (SqlDmlOperator) call;
    SqlNode targetTable = sqlDmlOperatorCall.getTargetTable();
    if (sqlDmlOperatorCall.getAlias() != null) {
      targetTable =
          SqlValidatorUtil.addAlias(targetTable, sqlDmlOperatorCall.getAlias().getSimple());
    }

    // For Merge On Read Case, we will want to run 'targetTable.*' to acquire all target cols in the
    // join. However, we must remove the 'EXTEND' from the target table call. order-by clause is
    // required for MOR Dml to guarantee records are sorted by system columns during join.
    // Our duplicate-check logic relies on this.
    if (sqlDmlOperatorCall.getDmlWriteMode() == RowLevelOperationMode.MERGE_ON_READ) {
      boolean isDelete = call.getKind().equals(SqlKind.DELETE);
      boolean isUpdate = call.getKind().equals(SqlKind.UPDATE);

      // orderByList we need will always be the system columns,
      // we already calculate this in 'getExtendedColumns' earlier

      if ((isUpdate
          || (isDelete && !((SqlDeleteFromTable) call).getPartitionColumns().isEmpty()))) {
        selectList = new SqlNodeList(SqlParserPos.ZERO);
        List<String> targetStarList =
            (sqlDmlOperatorCall.getAlias() != null)
                ? new ArrayList<>(sqlDmlOperatorCall.getAlias().names)
                : new ArrayList<>(List.copyOf(getTargetOperandNames(targetTable).names));
        targetStarList.add("");
        selectList.add(new SqlIdentifier(targetStarList, SqlParserPos.ZERO));
      }
    }

    SqlNode from = targetTable;
    SqlNode condition = sqlDmlOperatorCall.getCondition();

    SqlNode sourceTable = sqlDmlOperatorCall.getSourceTableRef();

    // join the source table with target table
    if (sourceTable != null) {
      SqlNode join =
          joinSourceAndTargetTable(
              targetTable,
              sourceTable,
              condition,
              condition == null ? JoinType.CROSS : JoinType.INNER);
      from = join;
      condition = null;
    }

    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        from,
        condition,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private SqlIdentifier getTargetOperandNames(SqlNode targetTable) {
    if (targetTable.getKind() == SqlKind.EXTEND) {
      return getTargetOperandNames(((SqlCall) targetTable).operand(0));
    }
    return (SqlIdentifier) targetTable;
  }

  /**
   * Special treatment for SqlUpdateTable . SqlUpdateTable is used by DML Update query to represent
   * updated rows With copy-on-write DML framework, updated rows are joined with original data so
   * that we could get a "copy" of original data with update values. Since original data already
   * contain user columns, there is no need for update rows to carry duplicated user columns.
   * Instead, only filePath and rowIndex are kept since they are join columns. Updated value columns
   * are also kept. <br>
   * <br>
   * With Merge_On_Read DML framework, we acquire the target table cols, system cols, and updated
   * source cols. During the physical plan {@link DmlPositionalMergeOnReadPlanGenerator#getPlan()}
   * The original target cols will be eventually replaced with update source cols if update matches.
   */
  @Override
  protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
    SqlSelect select = createSourceSelectForSqlDmlOperator(call);
    if (select == null) {
      return super.createSourceSelectForUpdate(call);
    }

    SqlNodeList selectList = select.getSelectList();

    // Add updated columns to the list
    int ordinal = 0;
    for (SqlNode exp : call.getSourceExpressionList()) {
      // Force unique aliases to avoid a duplicate for Y with
      // SET X=Y
      String alias = SqlUtil.deriveAliasFromOrdinal(ordinal);
      selectList.add(SqlValidatorUtil.addAlias(exp, alias));
      ++ordinal;
    }

    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        select.getFrom(),
        select.getWhere(),
        select.getGroup(),
        select.getHaving(),
        select.getWindowList(),
        select.getQualify(),
        select.getOrderList(),
        select.getOffset(),
        select.getFetch(),
        select.getHints());
  }

  /**
   * Special treatment for SqlDeleteFromTable . SqlDeleteFromTable is used by DML Delete query to
   * represent deleted rows With copy-on-write DML framework, deleted rows are joined with original
   * data so that we could get a "copy" of original data minus deleted values. Since original data
   * already contain user columns, there is no need for deleted rows to carry duplicated user
   * columns. Instead, only filePath and rowIndex are kept since they are join columns.
   *
   * <p>With Merge_On_Read DML framework, deleted rows are detailed by their system columns, which
   * are later projected during the Physical Plan in prep for a 'Delete File'
   */
  @Override
  protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
    SqlSelect select = createSourceSelectForSqlDmlOperator(call);
    return select != null ? select : super.createSourceSelectForDelete(call);
  }

  private void rewriteUpdate(SqlUpdate call) {
    SqlSelect select = createSourceSelectForUpdate(call);
    call.setSourceSelect(select);
  }

  public static boolean isSqlDmlOperatorWithStar(SqlCall call) {
    if (call == null) {
      return false;
    }
    Preconditions.checkState(call instanceof SqlDmlOperator);
    SqlDmlOperator sqlDmlOperatorCall = (SqlDmlOperator) call;

    return sqlDmlOperatorCall.getSourceTableRef() instanceof SqlIdentifier
        && ((SqlIdentifier) sqlDmlOperatorCall.getSourceTableRef()).isStar();
  }

  /**
   * Implementation of rewriteMerge based on Calcite's, with changes to ensure no nodes are reused
   * between SqlMerge and it's child SqlUpdate/SqlInsert nodes. Having a single node such as a
   * SqlSelect in multiple places in the parse tree leads to problems with SelectNamespace lookups
   * due to the SqlNode -> Namespace map.
   */
  private void rewriteMerge(SqlMerge call) {
    SqlNodeList selectList;
    SqlUpdate updateStmt = call.getUpdateCall();
    if (updateStmt != null) {
      // Rewrite the update first, to ensure we have a source select created for it
      rewriteUpdate(updateStmt);

      // if we have an update statement, just clone the select list
      // from the update statement's source since it's the same as
      // what we want for the select list of the merge source -- '*'
      // followed by the update set expressions
      selectList = (SqlNodeList) DeepCopier.copy(updateStmt.getSourceSelect().getSelectList());
    } else {
      // otherwise, just use select *
      selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    }
    SqlNode targetTable = call.getTargetTable();
    if (call.getAlias() != null) {
      targetTable = SqlValidatorUtil.addAlias(targetTable, call.getAlias().getSimple());
    }

    // Provided there is an insert substatement, the source select for
    // the merge is a left outer join between the source in the USING
    // clause and the target table; otherwise, the join is just an
    // inner join.  Need to clone the source table reference in order
    // for validation to work
    SqlNode sourceTableRef = call.getSourceTableRef();
    SqlInsert insertCall = call.getInsertCall();
    JoinType joinType = (insertCall == null) ? JoinType.INNER : JoinType.LEFT;

    SqlNode join =
        joinSourceAndTargetTable(targetTable, sourceTableRef, call.getCondition(), joinType);

    SqlSelect select =
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            selectList,
            join,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    call.setSourceSelect(select);

    if (isSqlDmlOperatorWithStar(updateStmt)) {
      // Set Update's source select based on Merge's source. Thus, validateUpdate would expand
      // Merge's source into concrete select items which
      // will be added to both Update's sourceExpressionList and Merge's source select
      SqlSelect updateSourceSelect =
          new SqlSelect(
              SqlParserPos.ZERO,
              null,
              selectList,
              call.getSourceTableRef(),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      updateStmt.setSourceSelect((SqlSelect) DeepCopier.copy(updateSourceSelect));
    }

    // Source for the insert call is a select of the source table
    // reference with the select list being the value expressions;
    // note that the values clause has already been converted to a
    // select on the values row constructor; so we need to extract
    // that via the from clause on the select
    if (insertCall != null) {
      boolean withStar = isSqlDmlOperatorWithStar(insertCall);
      if (withStar) {
        // use * as Insert's select
        selectList = new SqlNodeList(SqlParserPos.ZERO);
        selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
      } else {
        // insert values
        SqlCall valuesCall = (SqlCall) insertCall.getSource();
        SqlCall rowCall = valuesCall.operand(0);
        selectList = new SqlNodeList(rowCall.getOperandList(), SqlParserPos.ZERO);
      }

      final SqlNode insertSource = DeepCopier.copy(join);
      select =
          new SqlSelect(
              SqlParserPos.ZERO,
              null,
              selectList,
              insertSource,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      insertCall.setSource(select);
    }
  }

  /** Expander */
  private static class Expander extends SqlScopedShuttle {
    protected final org.apache.calcite.sql.validate.SqlValidatorImpl validator;

    Expander(org.apache.calcite.sql.validate.SqlValidatorImpl validator, SqlValidatorScope scope) {
      super(scope);
      this.validator = validator;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      SqlValidator validator = getScope().getValidator();
      // For UDFs we won't resolve to a function call if the parameter name is quoted,
      // so here is the workaround
      SqlIdentifier unquotedIdentifer = new SqlUnquotedIdentifier(id);
      final SqlCall call = validator.makeNullaryCall(unquotedIdentifer);
      if (call != null) {
        return (SqlNode) call.accept(this);
      } else {
        SqlIdentifier fqId = null;
        try {
          fqId = this.getScope().fullyQualify(id).identifier;
        } catch (CalciteContextException ex) {
          // The failure here may be happening because the path references a field within ANY type
          // column.
          // Check if the first derivable type in parents is ANY. If this is the case, fall back to
          // ITEM operator.
          // Otherwise, throw the original exception.
          if (id.names.size() > 1 && checkAnyType(id)) {
            SqlBasicCall itemCall =
                new SqlBasicCall(
                    SqlStdOperatorTable.ITEM,
                    new SqlNode[] {
                      id.getComponent(0, id.names.size() - 1),
                      SqlLiteral.createCharString(
                          (String) Util.last(id.names), id.getParserPosition())
                    },
                    id.getParserPosition());
            try {
              return itemCall.accept(this);
            } catch (Exception ignored) {
            }
          }
          throw ex;
        }
        SqlNode expandedExpr = fqId;
        if (DynamicRecordType.isDynamicStarColName((String) Util.last(fqId.names))
            && !DynamicRecordType.isDynamicStarColName((String) Util.last(id.names))) {
          SqlNode[] inputs =
              new SqlNode[] {
                fqId,
                SqlLiteral.createCharString((String) Util.last(id.names), id.getParserPosition())
              };
          SqlBasicCall item_call =
              new SqlBasicCall(SqlStdOperatorTable.ITEM, inputs, id.getParserPosition());
          expandedExpr = item_call;
        }

        this.validator.setOriginal((SqlNode) expandedExpr, id);
        return (SqlNode) expandedExpr;
      }
    }

    @Override
    protected SqlNode visitScoped(SqlCall call) {
      switch (call.getKind()) {
        case WITH:
        case SCALAR_QUERY:
        case CURRENT_VALUE:
        case NEXT_VALUE:
          return call;
        default:
          SqlCall newCall = call;
          if (call.getOperator() == SqlStdOperatorTable.DOT) {
            try {
              validator.deriveType(getScope(), call);
            } catch (Exception ex) {
              // The failure here may be happening because the dot operator was used within ANY type
              // column.
              // Check if the first derivable type in parents is ANY. If this is the case, fall back
              // to ITEM operator.
              // Otherwise, throw the original exception.
              if (checkAnyType(call)) {
                SqlNode left = call.getOperandList().get(0);
                SqlNode right = call.getOperandList().get(1);
                SqlNode[] inputs =
                    new SqlNode[] {
                      left, SqlLiteral.createCharString(right.toString(), call.getParserPosition())
                    };
                newCall =
                    new SqlBasicCall(SqlStdOperatorTable.ITEM, inputs, call.getParserPosition());
              } else {
                throw ex;
              }
            }
          }
          ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler(newCall, false);
          newCall.getOperator().acceptCall(this, newCall, true, argHandler);
          SqlNode result = (SqlNode) argHandler.result();
          this.validator.setOriginal(result, newCall);
          return result;
      }
    }

    private boolean checkAnyType(SqlIdentifier identifier) {
      List<String> names = identifier.names;
      for (int i = names.size(); i > 0; i--) {
        try {
          final RelDataType type =
              validator.deriveType(
                  getScope(),
                  new SqlIdentifier(names.subList(0, i), identifier.getParserPosition()));
          return SqlTypeName.ANY == type.getSqlTypeName();
        } catch (Exception ignored) {
        }
      }
      return false;
    }

    private boolean checkAnyType(SqlCall call) {
      if (call.getOperandList().size() == 0) {
        return false;
      }

      RelDataType type = null;
      final SqlNode operand = call.operand(0);
      try {
        type = validator.deriveType(getScope(), operand);
      } catch (Exception ignored) {
      }

      if (type != null) {
        return SqlTypeName.ANY == type.getSqlTypeName();
      }

      if (operand instanceof SqlCall) {
        return checkAnyType((SqlCall) operand);
      }

      if (operand instanceof SqlIdentifier) {
        return checkAnyType((SqlIdentifier) operand);
      }

      return false;
    }

    private static final class SqlUnquotedIdentifier extends SqlIdentifier {
      public SqlUnquotedIdentifier(SqlIdentifier identifier) {
        super(identifier.names, identifier.getCollation(), identifier.getParserPosition(), null);
      }

      @Override
      public boolean isComponentQuoted(int i) {
        return false;
      }
    }
  }

  private static final class ExtendedAliasExpander extends Expander {
    private final SqlSelect select;
    private final boolean allowMoreThanOneAlias;

    public ExtendedAliasExpander(
        org.apache.calcite.sql.validate.SqlValidatorImpl validator,
        SqlValidatorScope scope,
        SqlSelect select,
        boolean allowMoreThanOneAlias) {
      super(validator, scope);
      this.select = select;
      this.allowMoreThanOneAlias = allowMoreThanOneAlias;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (!id.isSimple()) {
        return super.visit(id);
      }

      try {
        return super.visit(id);
      } catch (CalciteContextException e) {
        // The failure here may be happening because the path references a field with an Alias.
        String name = id.getSimple();
        SqlNode expr = null;
        final SqlNameMatcher nameMatcher = validator.getCatalogReader().nameMatcher();
        int numAliases = 0;
        for (SqlNode s : select.getSelectList()) {
          final String alias = SqlValidatorUtil.getAlias(s, -1);
          if ((alias != null) && nameMatcher.matches(alias, name)) {
            expr = s;
            numAliases++;
          }
        }

        if (numAliases == 0) {
          return super.visit(id);
        }

        if ((numAliases > 1) && !allowMoreThanOneAlias) {
          // More than one column has this alias.
          throw validator.newValidationError(id, RESOURCE.columnAmbiguous(name));
        }

        expr = stripAs(expr);
        if (expr instanceof SqlIdentifier) {
          if (((SqlIdentifier) expr).names.equals(id.names)) {
            // Not an alias , don't want to update parser position
            return super.visit(id);
          }

          expr = getScope().fullyQualify((SqlIdentifier) expr).identifier;
        }

        validator.setOriginal(expr, id);
        return expr;
      }
    }
  }

  public static void checkForFeatureSpecificSyntax(SqlNode sqlNode, OptionResolver optionResolver) {
    sqlNode.accept(new CheckFeatureSpecificSyntaxEnabled(optionResolver));
  }

  /**
   * Implementation of a SqlVisitor which checks for SQL syntax elements that are gated behind
   * disabled feature flags and reports an error to the user if any are found.
   */
  private static class CheckFeatureSpecificSyntaxEnabled extends SqlBasicVisitor<Void> {

    private final OptionResolver optionResolver;

    public CheckFeatureSpecificSyntaxEnabled(OptionResolver optionResolver) {
      this.optionResolver = optionResolver;
    }

    @Override
    public Void visit(SqlCall call) {
      if (call instanceof SqlShowCreate) {
        boolean isView = ((SqlShowCreate) call).getIsView();
        checkFeatureEnabled(
            SHOW_CREATE_ENABLED,
            String.format(
                "\"SHOW CREATE %s <%s_name> [ AT ( REF[ERENCE] | BRANCH | TAG | COMMIT ) refValue ]\" syntax is disabled",
                isView ? "VIEW" : "TABLE", isView ? "view" : "table"));
      }

      return super.visit(call);
    }

    private void checkFeatureEnabled(TypeValidators.BooleanValidator validator, String message) {
      if (optionResolver != null && !optionResolver.getOption(validator)) {
        throw UserException.unsupportedError().message(message).buildSilently();
      }
    }
  }

  /** Deep copy implementation for SqlNodes. */
  public static class DeepCopier extends SqlShuttle {

    public static SqlNode copy(SqlNode node) {
      return node.accept(new DeepCopier());
    }

    @Override
    public SqlNode visit(SqlNodeList list) {
      SqlNodeList copy = new SqlNodeList(list.getParserPosition());
      for (SqlNode node : list) {
        copy.add(node.accept(this));
      }
      return copy;
    }

    // Override to copy all arguments regardless of whether visitor changes
    // them.
    @Override
    public SqlNode visit(SqlCall call) {
      ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler(call, true);
      call.getOperator().acceptCall(this, call, false, argHandler);
      return argHandler.result();
    }

    @Override
    public SqlNode visit(SqlLiteral literal) {
      return SqlNode.clone(literal);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      return SqlNode.clone(id);
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
      return SqlNode.clone(type);
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
      return SqlNode.clone(param);
    }

    @Override
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
      return SqlNode.clone(intervalQualifier);
    }
  }
}
