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
import com.dremio.exec.planner.types.RelDataTypeSystemImpl;
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
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
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
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
import org.apache.calcite.sql.type.BasicSqlType;
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
        SupportsSystemIcebergTables {

  private SqlNode extendedTargetTable;

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("COPY_INTO", SqlKind.OTHER) {

        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(
              operands.length == 7, "SqlCopyInto.createCall() has to get 7 operands!");
          return new SqlCopyIntoTable(
              pos,
              operands[0],
              operands[1],
              (SqlNodeList) operands[2],
              operands[3],
              operands[4],
              (SqlNodeList) operands[5],
              (SqlNodeList) operands[6]);
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
  private final SqlNode storageLocation;
  private final SqlNodeList files;
  private final SqlNode filePattern;
  private final SqlNode fileFormat;
  private final SqlNodeList optionsList;
  private final SqlNodeList optionsValueList;
  private SqlSelect sourceSelect;

  public SqlCopyIntoTable(
      SqlParserPos pos,
      SqlNode targetTable,
      SqlNode storageLocation,
      SqlNodeList files,
      SqlNode filePattern,
      SqlNode fileFormat,
      SqlNodeList optionsList,
      SqlNodeList optionsValueList) {
    super(pos);
    this.targetTable = targetTable;
    this.storageLocation = storageLocation;
    this.files = files;
    this.filePattern = filePattern;
    this.fileFormat = fileFormat;
    this.optionsList = optionsList;
    this.optionsValueList = optionsValueList;
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

  @Override
  public SqlNode getTargetTableWithoutExtendedCols() {
    return targetTable;
  }

  public void setSourceSelect(SqlSelect select) {
    this.sourceSelect = select;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(
        targetTable,
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
    writer.keyword("FROM");
    storageLocation.unparse(writer, leftPrec, rightPrec);
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
    // determine if the query was issued with an option ON_ERROR 'continue'
    if (isOnErrorHandlingRequested(getOptionsList(), getOptionsValueList())
        && extendedTargetTable == null) {
      extendedTargetTable = DmlUtils.extendTableWithCopyHistoryColumn(getTargetTable());
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
      RelNode inputRel,
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
    RelDataType rowType = getRowType(nsTable.getRowType(), cluster.getTypeFactory());
    return new CopyIntoTableCrel(
        cluster, cluster.traitSetOf(Convention.NONE), nsTable, rowType, copyIntoTableContext);
  }

  /**
   * Verify the COPY INTO command options to determine if the ON_ERROR 'continue' or 'skip_file'
   * option is specified.
   *
   * @param options command option names
   * @param optionValues command option values
   * @return true if the list of option name-value pairs contains the pair ON_ERROR: 'continue' or
   *     ON_ERROR: 'skip_file'
   */
  public static boolean isOnErrorHandlingRequested(
      List<String> options, List<Object> optionValues) {
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
    if (isOnErrorHandlingRequested(getOptionsList(), getOptionsValueList())) {
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
      rowType.getFieldList().forEach(builder::add);
      // extend with copy history column
      builder.add(
          new RelDataTypeFieldImpl(
              ColumnUtils.COPY_HISTORY_COLUMN_NAME,
              rowType.getFieldCount() + 1,
              new BasicSqlType(RelDataTypeSystemImpl.REL_DATA_TYPE_SYSTEM, SqlTypeName.VARCHAR)));
      return builder.build();
    }
    return rowType;
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
    return isOnErrorHandlingRequested(getOptionsList(), getOptionsValueList())
        ? SystemIcebergTableMetadataFactory.SUPPORTED_TABLES
        : Collections.EMPTY_LIST;
  }
}
