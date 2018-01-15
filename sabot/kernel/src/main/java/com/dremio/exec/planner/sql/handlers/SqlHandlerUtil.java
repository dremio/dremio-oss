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
package com.dremio.exec.planner.sql.handlers;

import static com.dremio.exec.planner.physical.PlannerSettings.QUERY_RESULTS_STORE_TABLE;
import static com.dremio.exec.planner.physical.PlannerSettings.STORE_QUERY_RESULTS;

import java.util.AbstractList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.text.StrTokenizer;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.StarColumnHelper;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.AbstractSchema;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.SchemaMutability.MutationType;
import com.dremio.exec.store.easy.arrow.ArrowFormatPlugin;
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
   * @throws RelConversionException If failed to convert the table definition into a RelNode.
   */
  public static RelNode resolveNewTableRel(boolean isNewTableView, List<String> tableFieldNames,
      RelDataType validatedRowtype, RelNode queryRelNode) throws ValidationException, RelConversionException {

    validateRowType(isNewTableView, tableFieldNames, validatedRowtype);
    if (tableFieldNames.size() > 0) {


      return MoreRelOptUtil.createRename(queryRelNode, tableFieldNames);
    }

    // As the column names of the view are derived from SELECT query, make sure the query has no duplicate column names
    ensureNoDuplicateColumnNames(validatedRowtype.getFieldNames());

    return queryRelNode;
  }

  public static void validateRowType(boolean isNewTableView, List<String> tableFieldNames,
      RelDataType validatedRowtype) throws ValidationException{

    // Get the row type of view definition query.
    // Reason for getting the row type from validated SqlNode than RelNode is because SqlNode -> RelNode involves
    // renaming duplicate fields which is not desired when creating a view or table.
    // For ex: SELECT region_id, region_id FROM cp.`region.json` LIMIT 1 returns
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

  public static Table getTableFromSchema(AbstractSchema schema, String tblName) {
    try {
      return schema.getTable(tblName);
    } catch (Exception e) {
      // TODO: Move to better exception types.
      throw new RuntimeException(
          String.format("Failure while trying to check if a table or view with given name [%s] already exists " +
              "in schema [%s]: %s", tblName, schema.getFullSchemaName(), e.getMessage()), e);
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
    final boolean storeResults = options.getOption(STORE_QUERY_RESULTS.getOptionName()) != null ?
        options.getOption(STORE_QUERY_RESULTS.getOptionName()).bool_val : false;

    if (!storeResults) {
      return inputRel;
    }

    // store query results as the system user
    final SchemaPlus systemUserSchema = context.getRootSchema(
        SchemaConfig.newBuilder(SystemUser.SYSTEM_USERNAME)
            .setProvider(context.getSchemaInfoProvider())
            .build());
    final String storeTablePath = options.getOption(QUERY_RESULTS_STORE_TABLE.getOptionName()).string_val;
    final List<String> storeTable =
        new StrTokenizer(storeTablePath, '.', config.quoting().string.charAt(0))
            .setIgnoreEmptyTokens(true)
            .getTokenList();

    final AbstractSchema schema = SchemaUtilities.resolveToMutableSchemaInstance(systemUserSchema,
        Util.skipLast(storeTable), true, MutationType.TABLE);

    // Query results are stored in arrow format. If need arises, we can change this to a configuration option.
    final Map<String, Object> storageOptions = ImmutableMap.<String, Object>of("type", ArrowFormatPlugin.ARROW_DEFAULT_NAME);

    final CreateTableEntry createTableEntry = schema.createNewTable(Util.last(storeTable), WriterOptions.DEFAULT, storageOptions);

    final RelTraitSet traits = inputRel.getCluster().traitSet().plus(Rel.LOGICAL);
    return new WriterRel(inputRel.getCluster(), traits, inputRel, createTableEntry, inputRel.getRowType());
  }
}
