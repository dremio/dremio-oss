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
import com.dremio.exec.store.dfs.system.SystemIcebergTableMetadataFactory.SupportedSystemIcebergTable;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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
  private final Map<String, TypeDef> transformationColNames = new HashMap<>();
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
    if (node instanceof SqlIdentifier) {
      SqlIdentifier identifier = (SqlIdentifier) node;
      return new SqlIdentifier(
          identifier.names.stream().map(this::prefixedVirtualColName).collect(Collectors.toList()),
          identifier.getParserPosition());
    } else if (node instanceof SqlBasicCall) {
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
        transformationColNames.put(
            ColumnUtils.COPY_HISTORY_COLUMN_NAME,
            new TypeDef(ColumnUtils.COPY_HISTORY_COLUMN_NAME, SqlTypeName.VARCHAR));
      }
      if (select != null) {
        if (!select.getKind().equals(SqlKind.SELECT)) {
          throw UserException.parseError()
              .message("Unsupported inner select type for COPY INTO operation")
              .buildSilently();
        }
        collectTransformationColNames(((SqlSelect) select).getSelectList().getList(), null);
      }
      extendedTargetTable = extendTargetTableWithColumns(getTargetTable(), transformationColNames);
    }
  }

  /**
   * Extends the given target table with additional columns.
   *
   * @param targetTable The {@link SqlNode} representing the target table to be extended.
   * @param cols A map of column names to {@link TypeDef} objects representing the columns to be
   *     added to the target table. The keys are the column names and the values are their
   *     corresponding type definitions.
   * @return A new {@link SqlNode} representing the target table extended with the specified
   *     columns. If the {@code cols} map is empty, the original {@code targetTable} is returned.
   */
  private SqlNode extendTargetTableWithColumns(SqlNode targetTable, Map<String, TypeDef> cols) {
    if (cols.isEmpty()) {
      return targetTable;
    }
    SqlParserPos pos = targetTable.getParserPosition();
    SqlNodeList nodes = new SqlNodeList(pos);
    cols.forEach((key, value) -> addColumn(nodes, pos, value));
    return SqlStdOperatorTable.EXTEND.createCall(pos, targetTable, nodes);
  }

  /**
   * Adds a column definition to the given list of SQL nodes.
   *
   * @param nodes The {@link SqlNodeList} to which the column definition will be added.
   * @param pos The {@link SqlParserPos} providing the parser position for the SQL nodes.
   * @param typeDef The {@link TypeDef} representing the type definition of the column to be added.
   *     If the {@code typeDef} has a non-null name, it will be added as a {@link SqlIdentifier}
   *     followed by its corresponding data type specification.
   */
  private void addColumn(SqlNodeList nodes, SqlParserPos pos, TypeDef typeDef) {
    if (typeDef.name != null) {
      nodes.add(new SqlIdentifier(typeDef.name, pos));
    }
    nodes.add(getDataTypeSpec(pos, typeDef));
  }

  /**
   * Generates a {@link SqlDataTypeSpec} based on the given {@link TypeDef}.
   *
   * @param pos The {@link SqlParserPos} providing the parser position for the SQL nodes.
   * @param typeDef The {@link TypeDef} representing the type definition for which the data type
   *     specification needs to be generated. If the type is {@code VARCHAR}, a {@link
   *     SqlBasicTypeNameSpec} is used. Otherwise, a {@link SqlComplexDataTypeSpec} is used to
   *     represent complex types like structs or lists.
   * @return A {@link SqlDataTypeSpec} that represents the SQL data type specification based on the
   *     provided {@code typeDef}.
   */
  private SqlDataTypeSpec getDataTypeSpec(SqlParserPos pos, TypeDef typeDef) {
    if (typeDef.type.equals(SqlTypeName.ANY) || typeDef.type.equals(SqlTypeName.VARCHAR)) {
      return new SqlDataTypeSpec(new SqlBasicTypeNameSpec(typeDef.type, -1, null, pos), pos);
    } else {
      // struct or list
      return new SqlComplexDataTypeSpec(
          new SqlDataTypeSpec(getUserDefinedTypeNameSpec(pos, typeDef), pos));
    }
  }

  /**
   * Generates a {@link SqlUserDefinedTypeNameSpec} based on the given {@link TypeDef}.
   *
   * @param pos The {@link SqlParserPos} providing the parser position for the SQL nodes.
   * @param typeDef The {@link TypeDef} representing the type definition for which the user-defined
   *     type name specification needs to be generated. The method handles both structured types
   *     (like structs) and array types.
   * @return A {@link SqlUserDefinedTypeNameSpec} that represents the SQL user-defined type name
   *     specification based on the provided {@code typeDef}.
   */
  private SqlUserDefinedTypeNameSpec getUserDefinedTypeNameSpec(SqlParserPos pos, TypeDef typeDef) {
    SqlTypeNameSpec typeNameSpec = null;
    if (typeDef.type.equals(SqlTypeName.ROW)) {
      List<SqlIdentifier> identifiers =
          typeDef.innerTypes.values().stream()
              .map(def -> new SqlIdentifier(def.name, pos))
              .collect(Collectors.toList());
      List<SqlComplexDataTypeSpec> complexDataTypeSpecs =
          typeDef.innerTypes.values().stream()
              .map(def -> new SqlComplexDataTypeSpec(getDataTypeSpec(pos, def)))
              .collect(Collectors.toList());
      typeNameSpec = new DremioSqlRowTypeSpec(pos, identifiers, complexDataTypeSpecs);
    } else {
      typeNameSpec =
          new SqlArrayTypeSpec(
              pos,
              new SqlComplexDataTypeSpec(
                  getDataTypeSpec(pos, typeDef.innerTypes.values().iterator().next())));
    }
    return new SqlUserDefinedTypeNameSpec(typeNameSpec, pos);
  }

  /**
   * Collects the transformation column names from the provided list of SQL nodes. This method
   * processes SQL identifiers and other function nodes to extract column names and their types, and
   * stores them in the `transformationColNames` map.
   *
   * @param nodes The list of SQL nodes to process.
   * @param innerTypeDef The inner type definition, used to handle nested structures such as arrays
   *     and structs.
   */
  private void collectTransformationColNames(List<SqlNode> nodes, TypeDef innerTypeDef) {
    for (SqlNode node : nodes) {
      if (node instanceof SqlIdentifier) {
        SqlIdentifier sqlIdentifier = (SqlIdentifier) node;
        String colName = prefixedVirtualColName(sqlIdentifier.names.get(0));
        TypeDef typeDef = new TypeDef(colName);
        if (innerTypeDef == null) {
          typeDef.type = SqlTypeName.ANY;
          transformationColNames.put(colName, typeDef);
        } else {
          typeDef = consolidateTypeDef(innerTypeDef, colName);
          transformationColNames.put(
              colName, mergeTypeDefs(transformationColNames.get(colName), typeDef));
        }
      } else if (node instanceof SqlBasicCall) {
        SqlBasicCall basicCall = (SqlBasicCall) node;
        if (basicCall.getOperator() instanceof SqlFunction) {
          // function
          collectTransformationColNames(basicCall.getOperandList(), innerTypeDef);
        } else if (basicCall.getOperandList().size() == 2
            && basicCall.getOperator().getKind().equals(SqlKind.OTHER_FUNCTION)) {
          // struct or list
          TypeDef typeDef = new TypeDef();
          if (basicCall.getOperandList().get(1) instanceof SqlNumericLiteral) {
            // list
            typeDef.type = SqlTypeName.ARRAY;
            typeDef.name = null;
          } else if (basicCall.getOperandList().get(1) instanceof SqlCharStringLiteral) {
            // struct
            typeDef.type = SqlTypeName.ROW;
            typeDef.name =
                ((SqlCharStringLiteral) basicCall.getOperandList().get(1))
                    .getNlsString()
                    .getValue();
          }
          if (innerTypeDef == null) {
            // leaf, it should be VARCHAR type
            typeDef.innerTypes.put(null, new TypeDef(null, SqlTypeName.ANY));
          } else {
            typeDef.innerTypes.put(innerTypeDef.name, innerTypeDef);
          }
          collectTransformationColNames(basicCall.getOperandList(), typeDef);
        } else {
          collectTransformationColNames(basicCall.getOperandList(), innerTypeDef);
        }
      }
    }
  }

  /**
   * Consolidates the given {@link TypeDef} by recursively processing its inner types. This method
   * updates the field name and manages nested type definitions.
   *
   * @param typeDef The {@link TypeDef} to consolidate.
   * @param fieldName The new field name to assign to the {@link TypeDef}.
   * @return The consolidated {@link TypeDef} with updated field name and nested types.
   */
  private TypeDef consolidateTypeDef(TypeDef typeDef, String fieldName) {
    if (!typeDef.innerTypes.isEmpty()) {
      TypeDef innerTypeDef = typeDef.innerTypes.entrySet().iterator().next().getValue();
      String oldInnerFieldName = innerTypeDef.name;
      TypeDef consolidatedTypeDef = consolidateTypeDef(innerTypeDef, typeDef.name);
      typeDef.name = fieldName;
      typeDef.innerTypes.remove(oldInnerFieldName);
      typeDef.innerTypes.put(consolidatedTypeDef.name, consolidatedTypeDef);
      return typeDef;
    } else {
      typeDef.name = fieldName;
      return typeDef;
    }
  }

  /**
   * Merges two {@link TypeDef} objects. If both type definitions have the same name, their inner
   * types are recursively merged. In case of type mismatch, an exception is thrown.
   *
   * @param left The first {@link TypeDef} to merge.
   * @param right The second {@link TypeDef} to merge.
   * @return The merged {@link TypeDef}. If either input is null, the other is returned. If both are
   *     null, null is returned.
   * @throws UserException If a type mismatch is found within the complex type definitions.
   */
  private TypeDef mergeTypeDefs(TypeDef left, TypeDef right) {
    if (left != null && right != null && Objects.equals(left.name, right.name)) {
      if (!left.type.equals(right.type)) {
        if ((left.type.equals(SqlTypeName.ROW) && right.type.equals(SqlTypeName.ARRAY))
            || (left.type.equals(SqlTypeName.ARRAY) && right.type.equals(SqlTypeName.ROW))) {
          throw UserException.parseError()
              .message("Type mismatch found within complex type %s definition.", left.name)
              .buildSilently();
        }
        if (right.type.equals(SqlTypeName.ROW) || right.type.equals(SqlTypeName.ARRAY)) {
          left.type = right.type;
        }
      }
      // we already have a typedef with the same key
      // merge inner type defs
      Map<String, TypeDef> mergedInnerTypes = new HashMap<>();
      for (Entry<String, TypeDef> entry : left.innerTypes.entrySet()) {
        if (right.innerTypes.containsKey(entry.getKey())) {
          TypeDef mergedInnerType =
              mergeTypeDefs(entry.getValue(), right.innerTypes.get(entry.getKey()));
          mergedInnerTypes.put(entry.getKey(), mergedInnerType);
        } else {
          mergedInnerTypes.put(entry.getKey(), entry.getValue());
        }
      }
      for (Entry<String, TypeDef> entry : right.innerTypes.entrySet()) {
        if (!left.innerTypes.containsKey(entry.getKey())) {
          mergedInnerTypes.put(entry.getKey(), entry.getValue());
        }
      }
      left.innerTypes.putAll(mergedInnerTypes);
      return left;
    } else if (left != null && right == null) {
      return left;
    } else {
      return right;
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
      transformationColNames.values().stream()
          .map(typeDef -> getTransformationsRowType(typeDef, typeFactory))
          .map(RelDataType::getFieldList)
          .forEach(builder::addAll);
      return builder.build();
    }
    return targetTableRowType;
  }

  /**
   * Generates a {@link RelDataType} based on the provided {@link TypeDef} and {@link
   * RelDataTypeFactory}. This method recursively handles complex types such as arrays and structs,
   * constructing the appropriate {@link RelDataType} structure.
   *
   * @param typeDef The {@link TypeDef} object representing the type definition.
   * @param typeFactory The {@link RelDataTypeFactory} used to create the {@link RelDataType}.
   * @return A {@link RelDataType} corresponding to the provided type definition.
   */
  private RelDataType getTransformationsRowType(TypeDef typeDef, RelDataTypeFactory typeFactory) {
    RelDataType relDataType = null;
    if (typeDef.type.equals(SqlTypeName.ANY) || typeDef.type.equals(SqlTypeName.VARCHAR)) {
      relDataType = typeFactory.createSqlType(typeDef.type);
    } else if (typeDef.type.equals(SqlTypeName.ARRAY)) {
      relDataType =
          typeFactory.createArrayType(
              getTransformationsRowType(typeDef.innerTypes.values().iterator().next(), typeFactory),
              -1L);
    } else if (typeDef.type.equals(SqlTypeName.ROW)) {
      List<RelDataType> relDataTypes =
          typeDef.innerTypes.values().stream()
              .map(def -> getTransformationsRowType(def, typeFactory))
              .collect(Collectors.toList());
      List<String> names = new ArrayList<>(typeDef.innerTypes.keySet());
      relDataType = typeFactory.createStructType(relDataTypes, names);
    }

    if (typeDef.name == null) {
      return relDataType;
    } else {
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
      builder.add(typeDef.name, relDataType);
      return builder.build();
    }
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
        ? Arrays.stream(SupportedSystemIcebergTable.values())
            .map(SupportedSystemIcebergTable::getTableName)
            .collect(Collectors.toList())
        : Collections.emptyList();
  }

  @Override
  public SqlSelect getTransformationsSelect() {
    return select != null ? sourceSelect : null;
  }

  /**
   * Represents a type definition with support for nested types. This class is used to define the
   * structure and type information of columns, including complex types such as arrays and structs.
   */
  private static final class TypeDef {
    private String name;
    private SqlTypeName type;
    private final Map<String, TypeDef> innerTypes = new HashMap<>();

    public TypeDef() {}

    public TypeDef(String name, SqlTypeName type) {
      this.name = name;
      this.type = type;
    }

    public TypeDef(String name) {
      this.name = name;
    }
  }
}
