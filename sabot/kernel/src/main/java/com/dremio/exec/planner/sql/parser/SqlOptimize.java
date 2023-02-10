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

import static com.dremio.exec.planner.OptimizeOutputSchema.getRelDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.handlers.query.OptimizeHandler;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

/**
 * SQL node tree for the internal <code>OPTIMIZE TABLE table_identifier</code> command.
 */
public class SqlOptimize extends SqlCall implements SqlToPlanHandler.Creator {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("OPTIMIZE", SqlKind.OTHER) {
      @Override
      public SqlCall createCall(SqlLiteral functionQualifier,
                                SqlParserPos pos, SqlNode... operands) {
        return new SqlOptimize(pos,
          (SqlIdentifier) operands[0],
          (SqlLiteral) operands[1],
          ((SqlLiteral) operands[2]).symbolValue(CompactionType.class),
          (SqlNode) operands[3],
          (SqlNodeList) operands[4],
          (SqlNodeList) operands[5]);
      }

      @Override
      public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        final RelDataTypeFactory typeFactory = validator.getTypeFactory();
        return getRelDataType(typeFactory);
      }
  };

  private static final List<String> OPTION_KEYS = ImmutableList.of(
    "target_file_size_mb",
    "max_file_size_mb",
    "min_file_size_mb",
    "min_input_files"
  );

  private SqlIdentifier table;
  private SqlLiteral rewriteManifests;
  private CompactionType compactionType;
  private SqlNode condition;
  private SqlNodeList optionsList;
  private SqlNodeList optionsValueList;
  private Long targetFileSize;
  private Long minFileSize;
  private Long maxFileSize;
  private Long minInputFiles;

  private SqlSelect sourceSelect;

  public SqlSelect getSourceSelect() {
    return sourceSelect;
  }


  /**
   * Creates a SqlOptimize.
   */
  public SqlOptimize(SqlParserPos pos,
                     SqlIdentifier table,
                     SqlLiteral rewriteManifests,
                     CompactionType compactionType,
                     SqlNode condition,
                     SqlNodeList optionsList,
                     SqlNodeList optionsValueList) {
    super(pos);
    this.table = table;
    this.rewriteManifests = rewriteManifests;
    this.compactionType = compactionType;
    this.condition = condition;
    this.optionsList = optionsList;
    this.optionsValueList = optionsValueList;

    populateOptions(optionsList, optionsValueList);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("OPTIMIZE");
    writer.keyword("TABLE");
    getTable().unparse(writer, leftPrec, rightPrec);
    if (!rewriteManifests.booleanValue()) {
      writer.keyword("REWRITE");
      writer.keyword("DATA");
      writer.keyword("USING");
      if (compactionType != CompactionType.SORT) {
        writer.keyword("BIN_PACK");
      } else {
        writer.keyword("SORT");
      }
      if (condition != null) {
        writer.keyword("WHERE");
        condition.unparse(writer, leftPrec, rightPrec);
      }
      if(optionsList != null) {
        writer.keyword("(");
        for (int i = 0; i < optionsList.size() - 1; i++) {
          optionsList.get(i).unparse(writer, leftPrec, rightPrec);
          writer.keyword("=");
          optionsValueList.get(i).unparse(writer, leftPrec, rightPrec);
          writer.keyword(",");
        }
        optionsList.get(optionsList.size()-1).unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        optionsValueList.get(optionsList.size()-1).unparse(writer, leftPrec, rightPrec);
        writer.keyword(")");
      }
    } else {
      writer.keyword("REWRITE");
      writer.keyword("MANIFESTS");
    }
  }

  @Override
  public void setOperand(int i, SqlNode operand) {
    switch (i) {
      case 0:
        table = (SqlIdentifier) operand;
        break;
      case 1:
        rewriteManifests = (SqlLiteral) operand;
        break;
      case 2:
        compactionType = ((SqlLiteral) operand).symbolValue(CompactionType.class);
        break;
      case 3:
        condition = operand;
        break;
      case 4:
        optionsList = (SqlNodeList) operand;
        break;
      case 5:
        optionsValueList = (SqlNodeList) operand;
      default:
        throw new AssertionError(i);
    }
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public void setSourceSelect(SqlSelect select) {
    this.sourceSelect = select;
  }

  @Override
  public List<SqlNode> getOperandList() {
    SqlLiteral compactionTypeSqlLiteral = SqlLiteral.createSymbol(compactionType, SqlParserPos.ZERO);
    return Collections.unmodifiableList(Arrays.asList(getTable(), rewriteManifests, compactionTypeSqlLiteral, condition, optionsList, optionsValueList));
  }

  @Override
  public SqlToPlanHandler toPlanHandler() {
    return new OptimizeHandler();
  }

  public NamespaceKey getPath() {
    return DmlUtils.getPath(getTable());
  }

  public SqlNode getCondition() {
    return condition;
  }

  public SqlIdentifier getTable() {
    return table;
  }

  public SqlLiteral getRewriteManifests() {
    return rewriteManifests;
  }

  public CompactionType getCompactionType() {
    return compactionType;
  }

  public SqlNodeList getOptionNames() {
    return optionsList;
  }

  public SqlNodeList getOptionValues() {
    return optionsValueList;
  }

  public Optional<Long> getTargetFileSize() {
    return Optional.ofNullable(targetFileSize);
  }

  public Optional<Long> getMinFileSize() {
    return Optional.ofNullable(minFileSize);
  }

  public Optional<Long> getMaxFileSize() {
    return Optional.ofNullable(maxFileSize);
  }

  public Optional<Long> getMinInputFiles() {
    return Optional.ofNullable(minInputFiles);
  }

  public List<String> getPartitionCol(DremioTable dremioTable) {
    return dremioTable.getDatasetConfig().getReadDefinition().getPartitionColumnsList();
  }

  private void populateOptions(SqlNodeList optionsList, SqlNodeList optionsValueList) {
    if (optionsList == null) {
      return;
    }

    int idx = 0;
    for (SqlNode option : optionsList) {
      SqlIdentifier optionIdentifier = (SqlIdentifier) option;
      String optionName = optionIdentifier.getSimple();
      SqlNumericLiteral optionValueNumLiteral = (SqlNumericLiteral) optionsValueList.get(idx);
      long optionValue;
      try {
        switch (OPTION_KEYS.indexOf(optionName.toLowerCase())) {
          case 0:
            optionValue = optionValueNumLiteral.longValue(true);
            this.targetFileSize = optionValue;
            break;
          case 1:
            optionValue = optionValueNumLiteral.longValue(true);
            this.maxFileSize = optionValue;
            break;
          case 2:
            optionValue = optionValueNumLiteral.longValue(true);
            this.minFileSize = optionValue;
            break;
          case 3:
            optionValue = optionValueNumLiteral.longValue(true);
            this.minInputFiles = optionValue;
            break;
          default:
            try {
              throw new SqlParseException(String.format("Unsupported option '%s' for OPTIMIZE TABLE.", optionName), pos, null, null, null);
            } catch (SqlParseException e) {
              throw new RuntimeException(e);
            }
        }
      } catch (CalciteException e) {
        throw new RuntimeException(new SqlParseException(String.format("Value of option '%s' must be an integer.", optionName), pos, null, null, null));
      }
      idx++;
    }
  }

  @Override
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validate(this.sourceSelect);
  }
}
