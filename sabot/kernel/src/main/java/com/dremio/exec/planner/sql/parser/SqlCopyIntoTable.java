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

import static com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.CopyOption.ON_ERROR;
import static com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.OnErrorAction.CONTINUE;
import static com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext.OnErrorAction.SKIP_FILE;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.calcite.logical.CopyIntoTableCrel;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.planner.sql.handlers.query.SupportsSqlToRelConversion;
import com.dremio.exec.planner.sql.handlers.query.SupportsSystemIcebergTables;
import com.dremio.exec.planner.sql.handlers.query.SupportsTransformation;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.ImmutableNullableList;

/** SQL node tree for the 'COPY INTO' command */
public class SqlCopyIntoTable extends SqlCall
    implements DataAdditionCmdCall,
        SqlDmlOperator,
        SupportsSqlToRelConversion,
        SupportsSystemIcebergTables,
        SupportsTransformation {

  private SqlNode extendedTargetTable;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("COPY_INTO", SqlKind.OTHER) {

        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 9, "SqlCopyInto.createCall() has to get 9 operands!");
          return new SqlCopyIntoTable(
              pos,
              operands[0],
              (SqlNodeList) operands[1],
              operands[2],
              operands[3],
              (SqlNodeList) operands[4],
              operands[5],
              operands[6],
              (SqlNodeList) operands[7],
              (SqlNodeList) operands[8]);
        }

        @Override
        public RelDataType deriveType(
            SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
          NamespaceKey path = DmlUtils.getPath(((SqlCopyIntoTable) call).getTargetTable());
          CatalogEntityKey.Builder keyBuilder =
              CatalogEntityKey.newBuilder()
                  .keyComponents(path.getPathComponents())
                  .tableVersionContext(DmlUtils.getVersionContext(call));
          DremioCatalogReader dremioCatalogReader =
              validator.getCatalogReader().unwrap(DremioCatalogReader.class);
          DremioTable dremioTable = dremioCatalogReader.getTable(keyBuilder.build());
          SqlValidatorTable nsTable =
              new DremioPrepareTable(dremioCatalogReader, validator.getTypeFactory(), dremioTable);
          if (nsTable == null) {
            throw UserException.invalidMetadataError()
                .message("Table with path %s cannot be found", path)
                .buildSilently();
          }
          return ((SqlCopyIntoTable) call)
              .getRowType(nsTable.getRowType(), validator.getTypeFactory());
        }
      };

  private final SqlNode targetTable;
  private final SqlNodeList mappings;
  private final SqlNode select;
  private final SqlNode storageLocation;
  private final SqlNodeList files;
  private final SqlNode filePattern;
  private final SqlNode fileFormat;
  private final SqlNodeList optionsList;
  private final SqlNodeList optionsValueList;
  private final Map<String, SqlTypeName> transformationColNames = new HashMap<>();
  private SqlSelect sourceSelect;

  public SqlCopyIntoTable(
      SqlParserPos pos,
      SqlNode targetTable,
      SqlNodeList mappings,
      SqlNode select,
      SqlNode storageLocation,
      SqlNodeList files,
      SqlNode filePattern,
      SqlNode fileFormat,
      SqlNodeList optionsList,
      SqlNodeList optionsValueList) {
    super(pos);
    this.targetTable = targetTable;
    this.mappings = mappings;
    this.select = select;
    this.storageLocation = storageLocation;
    this.files = files;
    this.filePattern = filePattern;
    this.fileFormat = fileFormat;
    this.optionsList = optionsList;
    this.optionsValueList = optionsValueList;
  }

  /**
   * Retrieves a list of renamed SELECT expressions from the current SqlNode, if applicable. This
   * method checks if the current SqlNode represents a SELECT statement (SqlKind.SELECT). If it
   * does, it extracts the SELECT list from the statement and applies the {@link #renameSelectNode}
   * method to each expression in the list. The renamed expressions are then returned as an Optional
   * containing a list of SqlNodes. If the current SqlNode is not a SELECT statement, or the SELECT
   * node is null, this method returns an empty list.
   *
   * @return a list of renamed SELECT expressions, or empty if not applicable
   */
  public List<SqlNode> getSelectNodes() {
    if (select != null) {
      if (!select.getKind().equals(SqlKind.SELECT)) {
        throw UserException.parseError()
            .message("Unsupported inner select type for COPY INTO operation")
            .buildSilently();
      }
      SqlSelect sqlSelect = (SqlSelect) select;
      return sqlSelect.getSelectList().getList().stream()
          .map(this::renameSelectNode)
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  /**
   * Renames the identifiers within a SqlNode representing a SELECT statement. This method
   * recursively traverses the SqlNode tree, specifically targeting identifiers and basic calls. For
   * identifiers (SqlIdentifier), it prepends each identifier segment with {@link
   * ColumnUtils#VIRTUAL_COLUMN_PREFIX} and converts it to uppercase. For other functions
   * (SqlBasicCall), it applies the renaming logic to each operand within the call. SqlNodes that
   * are not identifiers or basic calls are left unchanged.
   *
   * @param node the SqlNode representing the SELECT statement to be renamed
   * @return a new SqlNode with renamed identifiers (if applicable)
   */
  private SqlNode renameSelectNode(SqlNode node) {
    if (node.getKind().equals(SqlKind.IDENTIFIER)) {
      SqlIdentifier identifier = (SqlIdentifier) node;
      return new SqlIdentifier(
          identifier.names.stream().map(this::prefixedVirtualColName).collect(Collectors.toList()),
          identifier.getParserPosition());
    } else if (node.getKind().equals(SqlKind.OTHER_FUNCTION)) {
      SqlBasicCall sqlBasicCall = (SqlBasicCall) node;
      List<SqlNode> operandList = sqlBasicCall.getOperandList();
      return new SqlBasicCall(
          sqlBasicCall.getOperator(),
          operandList.stream().map(this::renameSelectNode).toArray(SqlNode[]::new),
          sqlBasicCall.getParserPosition());
    }
    return node;
  }

  public SqlNodeList getMappings() {
    return mappings;
  }

  private String prefixedVirtualColName(String colName) {
    return ColumnUtils.VIRTUAL_COLUMN_PREFIX + colName.toUpperCase();
  }

  public String getStorageLocation() {
    return ((SqlLiteral) storageLocation).toValue();
  }

  public List<String> getFiles() {
    return files.getList().stream()
        .map(x -> ((SqlLiteral) x).toValue())
        .collect(Collectors.toList());
  }

  public Optional<String> getFilePattern() {
    if (filePattern == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(((SqlLiteral) filePattern).toValue());
  }

  public Optional<String> getFileFormat() {
    if (fileFormat == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(((SqlLiteral) fileFormat).toValue());
  }

  public List<String> getOptionsList() {
    return optionsList.getList().stream()
        .map(x -> ((SqlLiteral) x).toValue())
        .collect(Collectors.toList());
  }

  public List<Object> getOptionsValueList() {
    return optionsValueList.getList().stream()
        .map(
            value -> {
              if (value instanceof SqlNodeList) {
                SqlNodeList listValues = (SqlNodeList) value;
                return listValues.getList().stream()
                    .map(x -> ((SqlLiteral) x).toValue())
                    .collect(Collectors.toList());
              } else if (value instanceof SqlNode) {
                return ((SqlLiteral) value).toValue();
              } else {
                throw UserException.parseError()
                    .message("Specified value '%s' is not valid ", value)
                    .buildSilently();
              }
            })
        .collect(Collectors.toList());
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public SqlNode getTargetTable() {
    return extendedTargetTable == null ? targetTable : extendedTargetTable;
  }

  public void setSourceSelect(SqlSelect select) {
    this.sourceSelect = select;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(
        targetTable,
        mappings,
        select,
        storageLocation,
        files,
        filePattern,
        fileFormat,
        optionsList,
        optionsValueList);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("COPY INTO");
    targetTable.unparse(writer, leftPrec, rightPrec);
    if (mappings != null && !SqlNodeList.isEmptyList(mappings)) {
      writer.keyword("(");
      mappings.unparse(writer, leftPrec, rightPrec);
      writer.keyword(")");
    }
    writer.keyword("FROM");
    if (select != null) {
      writer.keyword("(");
      writer.keyword("SELECT");
      select.unparse(writer, leftPrec, rightPrec);
      writer.keyword("FROM");
      storageLocation.unparse(writer, leftPrec, rightPrec);
      writer.keyword(")");
    } else {
      storageLocation.unparse(writer, leftPrec, rightPrec);
    }
    if (files.size() > 0) {
      writer.keyword("FILES");
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, files);
    }
    if (filePattern != null) {
      writer.keyword("REGEX");
      filePattern.unparse(writer, leftPrec, rightPrec);
    }
    if (fileFormat != null) {
      writer.keyword("FILE_FORMAT");
      fileFormat.unparse(writer, leftPrec, rightPrec);
    }
    if (optionsList != null && optionsList.size() > 0) {
      writer.keyword("(");
      for (int i = 0; i < optionsList.size(); i++) {
        if (i > 0) {
          writer.keyword(",");
        }
        optionsList.get(i).unparse(writer, leftPrec, rightPrec);
        Object value = optionsValueList.get(i);
        if (value instanceof SqlNodeList) {
          SqlNodeList listValues = (SqlNodeList) value;
          writer.keyword("(");
          for (int j = 0; j < listValues.size(); j++) {
            SqlNode listValue = listValues.get(j);
            if (j > 0) {
              writer.keyword(",");
            }
            listValue.unparse(writer, leftPrec, rightPrec);
          }
          writer.keyword(")");
        } else {
          ((SqlNode) value).unparse(writer, leftPrec, rightPrec);
        }
      }
      writer.keyword(")");
    }
  }

  @Override
  public void extendTableWithDataFileSystemColumns() {
    if (extendedTargetTable == null) {
      // determine if the query was issued with an option ON_ERROR 'continue'
      if (isOnErrorHandlingRequested()) {
        transformationColNames.put(ColumnUtils.COPY_HISTORY_COLUMN_NAME, SqlTypeName.VARCHAR);
      }
      if (select != null) {
        if (!select.getKind().equals(SqlKind.SELECT)) {
          throw UserException.parseError()
              .message("Unsupported inner select type for COPY INTO operation")
              .buildSilently();
        }
        collectTransformationColNames(((SqlSelect) select).getSelectList().getList());
      }
      extendedTargetTable =
          DmlUtils.extendTableWithColumns(getTargetTable(), transformationColNames);
    }
  }

  /**
   * Extracts virtual column names and data types from a list of SqlNodes representing a SELECT
   * statement. This method recursively traverses the provided nodes list, specifically targeting
   * identifiers and other functions.It extracts the identifier name (plus the prefix) and adds it
   * to the extendedColNames map with a data type of {@link SqlTypeName#VARCHAR}. For other
   * functions (SqlBasicCall), it recursively calls itself on the operand list to extract virtual
   * column names from within the call. SqlNodes that are not identifiers or basic calls are ignored
   * in this process.
   *
   * @param nodes the list of SqlNodes representing the SELECT statement
   */
  private void collectTransformationColNames(List<SqlNode> nodes) {
    for (SqlNode node : nodes) {
      if (node.getKind().equals(SqlKind.IDENTIFIER)) {
        ((SqlIdentifier) node)
            .names.forEach(
                n -> transformationColNames.put(prefixedVirtualColName(n), SqlTypeName.VARCHAR));
      } else if (node.getKind().equals(SqlKind.OTHER_FUNCTION)) {
        collectTransformationColNames(((SqlBasicCall) node).getOperandList());
      }
    }
  }

  @Override
  public SqlNode getSourceTableRef() {
    return null;
  }

  @Override
  public SqlIdentifier getAlias() {
    throw new UnsupportedOperationException("Alias is not supported for CopyInto");
  }

  @Override
  public SqlNode getCondition() {
    throw new UnsupportedOperationException("Condition is not supported for CopyInto");
  }

  @Override
  public List<PartitionTransform> getPartitionTransforms(DremioTable dremioTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getSortColumns() {
    return Lists.newArrayList();
  }

  @Override
  public List<String> getDistributionColumns() {
    return Lists.newArrayList();
  }

  @Override
  public boolean isSingleWriter() {
    return false;
  }

  @Override
  public List<String> getFieldNames() {
    return Lists.newArrayList();
  }

  @Override
  public SqlNode getQuery() {
    return this;
  }

  @Override
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validate(this.sourceSelect);
  }

  @Override
  public RelNode convertToRel(
      RelOptCluster cluster,
      Prepare.CatalogReader catalogReader,
      RelNode relNode,
      RelOptTable.ToRelContext relContext) {
    CatalogEntityKey.Builder keyBuilder =
        CatalogEntityKey.newBuilder()
            .keyComponents(getPath().getPathComponents())
            .tableVersionContext(DmlUtils.getVersionContext((SqlNode) this));
    DremioCatalogReader dremioCatalogReader = catalogReader.unwrap(DremioCatalogReader.class);
    DremioTable dremioTable = dremioCatalogReader.getTable(keyBuilder.build());
    Prepare.PreparingTable nsTable =
        new DremioPrepareTable(dremioCatalogReader, cluster.getTypeFactory(), dremioTable);
    CopyIntoTableContext copyIntoTableContext = new CopyIntoTableContext(this);
    RelDataType rowType = nsTable.getRowType();
    RelDataType targetTableRowType = getRowType(rowType, cluster.getTypeFactory());
    RelDataType transformationsRowType =
        getTransformationsRowType(rowType, cluster.getTypeFactory());

    return new CopyIntoTableCrel(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        nsTable,
        relNode,
        targetTableRowType,
        transformationsRowType,
        copyIntoTableContext);
  }

  /**
   * Verify the COPY INTO command options to determine if the ON_ERROR 'continue' or 'skip_file'
   * option is specified.
   *
   * @return true if the list of option name-value pairs contains the pair ON_ERROR: 'continue' or
   *     ON_ERROR: 'skip_file'
   */
  public boolean isOnErrorHandlingRequested() {
    List<String> options = getOptionsList();
    List<Object> optionValues = getOptionsValueList();

    if (options.size() != optionValues.size()) {
      return false;
    }
    return IntStream.range(0, options.size())
        .anyMatch(
            i -> {
              if (options.get(i).equalsIgnoreCase(ON_ERROR.name())) {
                String value = (String) optionValues.get(i);
                return value.equalsIgnoreCase(CONTINUE.name())
                    || value.equalsIgnoreCase(SKIP_FILE.name());
              }
              return false;
            });
  }

  /**
   * If the command is executed with the ON_ERROR 'continue' option, augment the original table
   * rowType object by adding an 'error 'column.
   *
   * @param rowType original table rowType
   * @param typeFactory type factory for instantiating SQL types
   * @return decorated rowType
   */
  private RelDataType getRowType(RelDataType rowType, RelDataTypeFactory typeFactory) {
    if (isOnErrorHandlingRequested()) {
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
      rowType.getFieldList().forEach(builder::add);
      // extend with copy history column
      return builder.add(ColumnUtils.COPY_HISTORY_COLUMN_NAME, SqlTypeName.VARCHAR).build();
    }
    return rowType;
  }

  /**
   * Constructs a new RelDataType representing the row type of the virtual column definitions from
   * transformations and if applicable the error column.
   *
   * @param targetTableRowType the row type of the target iceberg table
   * @param typeFactory the RelDataTypeFactory used to create the new data type
   * @return the new RelDataType representing the row type after transformations, or null if no
   *     transformations are applied.
   */
  private RelDataType getTransformationsRowType(
      RelDataType targetTableRowType, RelDataTypeFactory typeFactory) {
    if (select != null && !transformationColNames.isEmpty()) {
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
      builder.addAll(targetTableRowType.getFieldList());
      transformationColNames.forEach(builder::add);
      return builder.build();
    }
    return null;
  }

  @Override
  public SqlTableVersionSpec getSqlTableVersionSpec() {
    return null;
  }

  @Override
  public TableVersionSpec getTableVersionSpec() {
    if (targetTable instanceof SqlVersionedTableCollectionCall) {
      return ((SqlVersionedTableCollectionCall) targetTable)
          .getVersionedTableMacroCall()
          .getSqlTableVersionSpec()
          .getTableVersionSpec();
    }
    return null;
  }

  @Override
  public List<String> systemTableNames() {
    // Assesses if query has ON_ERROR (CONTINUE) option, if so returns the names of the internal
    // copy error tables.
    return isOnErrorHandlingRequested()
        ? SystemIcebergTableMetadataFactory.SUPPORTED_TABLES
        : Collections.EMPTY_LIST;
  }

  @Override
  public SqlSelect getTransformationsSelect() {
    return select != null ? sourceSelect : null;
  }

  public List<String> getTransformationColNames() {
    return new ArrayList<>(transformationColNames.keySet());
  }
}
