
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
package com.dremio.exec.planner.sql.handlers;

import static com.dremio.exec.planner.physical.PlannerSettings.QUERY_RESULTS_STORE_TABLE;
import static com.dremio.exec.planner.physical.PlannerSettings.STORE_QUERY_RESULTS;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.lang3.text.StrTokenizer;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.StarColumnHelper;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PlannerSettings.StoreQueryResultsPolicy;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.parser.SqlColumnDeclaration;
import com.dremio.exec.planner.types.RelDataTypeSystemImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.easy.arrow.ArrowFormatPlugin;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SqlHandlerUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlHandlerUtil.class);

  /**
   * Resolve final RelNode of the new table (or view) for given table field list and new table definition.
   *
   * @param isNewTableView Is the new table created a view? This doesn't affect the functionality, but it helps format
   *                       better error messages.
   * @param tableFieldNames List of fields specified in new table/view field list. These are the fields given just after
   *                        new table name.
   *                        Ex. CREATE TABLE newTblName(col1, medianOfCol2, avgOfCol3) AS
   *                        SELECT col1, median(col2), avg(col3) FROM sourcetbl GROUP BY col1;
   * @throws ValidationException If table's fields list and field list specified in table definition are not valid.
   */
  public static RelNode resolveNewTableRel(boolean isNewTableView, List<String> tableFieldNames,
                                           RelDataType validatedRowtype, RelNode queryRelNode) throws ValidationException {
    return resolveNewTableRel(isNewTableView, tableFieldNames, validatedRowtype, queryRelNode,
        false /* disallow duplicates cols in select query */);
  }

  public static RelNode resolveNewTableRel(boolean isNewTableView, List<String> tableFieldNames,
                                           RelDataType validatedRowtype, RelNode queryRelNode,
                                           boolean allowDuplicatesInSelect) throws ValidationException {

    validateRowType(isNewTableView, tableFieldNames, validatedRowtype);
    if (tableFieldNames.size() > 0) {
      return MoreRelOptUtil.createRename(queryRelNode, tableFieldNames);
    }

    if (!allowDuplicatesInSelect) {
      ensureNoDuplicateColumnNames(validatedRowtype.getFieldNames());
    }

    return queryRelNode;
  }

  public static void validateRowType(boolean isNewTableView, List<String> tableFieldNames,
      RelDataType validatedRowtype) throws ValidationException{

    // Get the row type of view definition query.
    // Reason for getting the row type from validated SqlNode than RelNode is because SqlNode -> RelNode involves
    // renaming duplicate fields which is not desired when creating a view or table.
    // For ex: SELECT region_id, region_id FROM cp."region.json" LIMIT 1 returns
    //  +------------+------------+
    //  | region_id  | region_id0 |
    //  +------------+------------+
    //  | 0          | 0          |
    //  +------------+------------+
    // which is not desired when creating new views or tables.
//    final RelDataType queryRowType = validatedRowtype;

    if (tableFieldNames.size() > 0) {
      // Field count should match.
      if (tableFieldNames.size() != validatedRowtype.getFieldCount()) {
        final String tblType = isNewTableView ? "view" : "table";
        throw UserException.validationError()
            .message("%s's field list and the %s's query field list have different counts.", tblType, tblType)
            .build(logger);
      }

      // CTAS's query field list shouldn't have "*" when table's field list is specified.
      for (String field : validatedRowtype.getFieldNames()) {
        if (field.equals("*")) {
          final String tblType = isNewTableView ? "view" : "table";
          throw UserException.validationError()
              .message("%s's query field list has a '*', which is invalid when %s's field list is specified.",
                  tblType, tblType)
              .build(logger);
        }
      }
    }

    // validate the given field names to make sure there are no duplicates
    ensureNoDuplicateColumnNames(tableFieldNames);
  }

  private static void ensureNoDuplicateColumnNames(List<String> fieldNames) throws ValidationException {
    final HashSet<String> fieldHashSet = Sets.newHashSetWithExpectedSize(fieldNames.size());
    for(String field : fieldNames) {
      if (fieldHashSet.contains(field.toLowerCase())) {
        throw new ValidationException(String.format("Duplicate column name [%s]", field));
      }
      fieldHashSet.add(field.toLowerCase());
    }
  }

  /**
   *  Resolve the partition columns specified in "PARTITION BY" clause of CTAS statement.
   *
   *  A partition column is resolved, either (1) the same column appear in the select list of CTAS
   *  or (2) CTAS has a * in select list.
   *
   *  In the second case, a PROJECT with ITEM expression would be created and returned.
   *  Throw validation error if a partition column is not resolved correctly.
   *
   * @param input : the RelNode represents the select statement in CTAS.
   * @param partitionColumns : the list of partition columns.
   * @return : 1) the original RelNode input, if all partition columns are in select list of CTAS
   *           2) a New Project, if a partition column is resolved to * column in select list
   *           3) validation error, if partition column is not resolved.
   */
  public static RelNode qualifyPartitionCol(RelNode input, List<String> partitionColumns) {

    final RelDataType inputRowType = input.getRowType();

    final List<RexNode> colRefStarExprs = Lists.newArrayList();
    final List<String> colRefStarNames = Lists.newArrayList();
    final RexBuilder builder = input.getCluster().getRexBuilder();
    final int originalFieldSize = inputRowType.getFieldCount();

    for (final String col : partitionColumns) {
      final RelDataTypeField field = inputRowType.getField(col, false, false);

      if (field == null) {
        throw UserException.validationError()
            .message("Partition column %s is not in the SELECT list of CTAS!", col)
            .build(logger);
      } else {
        if (field.getName().startsWith(StarColumnHelper.STAR_COLUMN)) {
          colRefStarNames.add(col);

          final List<RexNode> operands = Lists.newArrayList();
          operands.add(new RexInputRef(field.getIndex(), field.getType()));
          operands.add(builder.makeLiteral(col));
          final RexNode item = builder.makeCall(SqlStdOperatorTable.ITEM, operands);
          colRefStarExprs.add(item);
        }
      }
    }

    if (colRefStarExprs.isEmpty()) {
      return input;
    } else {
      final List<String> names =
          new AbstractList<String>() {
            @Override
            public String get(int index) {
              if (index < originalFieldSize) {
                return inputRowType.getFieldNames().get(index);
              } else {
                return colRefStarNames.get(index - originalFieldSize);
              }
            }

            @Override
            public int size() {
              return originalFieldSize + colRefStarExprs.size();
            }
          };

      final List<RexNode> refs =
          new AbstractList<RexNode>() {
            @Override
            public int size() {
              return originalFieldSize + colRefStarExprs.size();
            }

            @Override
            public RexNode get(int index) {
              if (index < originalFieldSize) {
                return RexInputRef.of(index, inputRowType.getFieldList());
              } else {
                return colRefStarExprs.get(index - originalFieldSize);
              }
            }
          };

      return RelOptUtil.createProject(input, refs, names, false);
    }
  }

  public static void unparseSqlNodeList(SqlWriter writer, int leftPrec, int rightPrec, SqlNodeList fieldList) {
    writer.keyword("(");
    fieldList.get(0).unparse(writer, leftPrec, rightPrec);
    for (int i = 1; i<fieldList.size(); i++) {
      writer.keyword(",");
      fieldList.get(i).unparse(writer, leftPrec, rightPrec);
    }
    writer.keyword(")");
  }

  /**
   * When enabled, add a writer rel on top of the given rel to catch the output and write to configured store table.
   * @param inputRel
   * @return
   */
  public static Rel storeQueryResultsIfNeeded(final SqlParser.Config config, final QueryContext context,
                                              final Rel inputRel) {
    final OptionManager options = context.getOptions();
    final StoreQueryResultsPolicy storeQueryResultsPolicy = Optional
        .ofNullable(options.getOption(STORE_QUERY_RESULTS.getOptionName()))
        .map(o -> StoreQueryResultsPolicy.valueOf(o.getStringVal().toUpperCase(Locale.ROOT)))
        .orElse(StoreQueryResultsPolicy.NO);

    switch (storeQueryResultsPolicy) {
    case NO:
      return inputRel;

    case DIRECT_PATH:
    case PATH_AND_ATTEMPT_ID:
      // supported cases
      break;

    default:
      logger.warn("Unknown query result store policy {}. Query results won't be saved", storeQueryResultsPolicy);
      return inputRel;
    }

    final String storeTablePath = options.getOption(QUERY_RESULTS_STORE_TABLE.getOptionName()).getStringVal();
    final List<String> storeTable =
        new StrTokenizer(storeTablePath, '.', config.quoting().string.charAt(0))
            .setIgnoreEmptyTokens(true)
            .getTokenList();

    if (storeQueryResultsPolicy == StoreQueryResultsPolicy.PATH_AND_ATTEMPT_ID) {
      // QueryId is same as attempt id. Using its string form for the table name
      storeTable.add(QueryIdHelper.getQueryId(context.getQueryId()));
    }

    // Query results are stored in arrow format. If need arises, we can change this to a configuration option.
    final Map<String, Object> storageOptions = ImmutableMap.<String, Object>of("type", ArrowFormatPlugin.ARROW_DEFAULT_NAME);

    WriterOptions writerOptions = WriterOptions.DEFAULT;
    if (options.getOption(PlannerSettings.ENABLE_OUTPUT_LIMITS)) {
      writerOptions = WriterOptions.DEFAULT
                                   .withOutputLimitEnabled(options.getOption(PlannerSettings.ENABLE_OUTPUT_LIMITS))
                                   .withOutputLimitSize(options.getOption(PlannerSettings.OUTPUT_LIMIT_SIZE));
    }

    // store table as system user.
    final CreateTableEntry createTableEntry = context.getCatalog()
        .resolveCatalog(SystemUser.SYSTEM_USERNAME)
        .createNewTable(new NamespaceKey(storeTable), null, writerOptions, storageOptions);

    final RelTraitSet traits = inputRel.getCluster().traitSet().plus(Rel.LOGICAL);
    return new WriterRel(inputRel.getCluster(), traits, inputRel, createTableEntry, inputRel.getRowType());
  }

  /**
   * Checks if new columns list has duplicates or has columns from existing schema
   * @param newColumsDeclaration
   * @param existingSchema
   */
  public static void checkForDuplicateColumns(List<SqlColumnDeclaration> newColumsDeclaration, BatchSchema existingSchema, String sql) {
    Set<String> existingColumns = existingSchema.getFields().stream().map(Field::getName).map(String::toUpperCase)
        .collect(Collectors.toSet());
    Set<String> newColumns = new HashSet<>();
    String column;
    for (SqlColumnDeclaration columnDecl : newColumsDeclaration) {
      column = columnDecl.getName().getSimple().toUpperCase();
      if (existingColumns.contains(column)) {
        throw SqlExceptionHelper.parseError(String.format("Column [%s] already in the table.", column), sql,
            columnDecl.getParserPosition()).buildSilently();
      }
      if (newColumns.contains(column)) {
        throw SqlExceptionHelper.parseError(String.format("Column [%s] specified multiple times.", column), sql,
            columnDecl.getParserPosition()).buildSilently();
      }
      newColumns.add(column);
    }
  }

  /**
   * Create arrow field from sql column declaration
   *
   * @param config
   * @param column
   * @return
   */
  public static Field fieldFromSqlColDeclaration(SqlHandlerConfig config, SqlColumnDeclaration column, String sql) {
    if (SqlTypeName.get(column.getDataType().getTypeName().getSimple()) == null) {
      throw SqlExceptionHelper.parseError(String.format("Invalid column type [%s] specified for column [%s].",
          column.getDataType(), column.getName().getSimple()),
          sql, column.getParserPosition()).buildSilently();
    }

    if (SqlTypeName.get(column.getDataType().getTypeName().getSimple()) == SqlTypeName.DECIMAL &&
        column.getDataType().getPrecision() > RelDataTypeSystemImpl.MAX_NUMERIC_PRECISION) {
      throw SqlExceptionHelper.parseError(String.format("Precision larger than %s is not supported.",
          RelDataTypeSystemImpl.MAX_NUMERIC_PRECISION), sql, column.getParserPosition()).buildSilently();
    }

    if (SqlTypeName.get(column.getDataType().getTypeName().getSimple()) == SqlTypeName.DECIMAL &&
        column.getDataType().getScale() > RelDataTypeSystemImpl.MAX_NUMERIC_SCALE) {
      throw SqlExceptionHelper.parseError(String.format("Scale larger than %s is not supported.",
          RelDataTypeSystemImpl.MAX_NUMERIC_SCALE), sql, column.getParserPosition()).buildSilently();
    }

    return CalciteArrowHelper.fieldFromCalciteRowType(column.getName().getSimple(), column.getDataType()
        .deriveType(config.getConverter().getTypeFactory())).orElseThrow(
        () -> SqlExceptionHelper.parseError(String.format("Invalid type [%s] specified for column [%s].",
            column.getDataType(), column.getName().getSimple()), sql, column.getParserPosition()).buildSilently());
  }

  /**
   * create BatchSchema from table schema specified in sql as list of columns
   * @param config
   * @param newColumsDeclaration
   * @return
   */
  public static BatchSchema batchSchemaFromSqlSchemaSpec(SqlHandlerConfig config, List<SqlColumnDeclaration> newColumsDeclaration, String sql) {
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (SqlColumnDeclaration column : newColumsDeclaration) {
      schemaBuilder.addField(fieldFromSqlColDeclaration(config, column, sql));
    }
    return schemaBuilder.build();
  }

  /**
   * create sql column declarations from SqlNodeList
   */
  public static List<SqlColumnDeclaration> columnDeclarationsFromSqlNodes(SqlNodeList columnList, String sql) {
    List<SqlColumnDeclaration> columnDeclarations = new ArrayList<>();
    for (SqlNode node : columnList.getList()) {
      if (node instanceof SqlColumnDeclaration) {
        columnDeclarations.add((SqlColumnDeclaration) node);
      } else {
        throw SqlExceptionHelper.parseError("Column type not specified", sql, node.getParserPosition()).buildSilently();
      }
    }
    return columnDeclarations;
  }
}
