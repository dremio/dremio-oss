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

import static com.dremio.exec.planner.VacuumOutputSchema.getTableOutputRelDataType;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.exec.calcite.logical.VacuumTableCrel;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.handlers.query.SupportsSqlToRelConversion;
import com.dremio.exec.planner.sql.handlers.query.VacuumTableHandler;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;


public class SqlVacuumTable extends SqlVacuum implements SqlToPlanHandler.Creator, SupportsSqlToRelConversion {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("VACUUM", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 5, "SqlVacuumTable.createCall() " +
        "has 3 operands!");
      return new SqlVacuumTable(
        pos,
        (SqlIdentifier) operands[0],
        (SqlLiteral) operands[1],
        (SqlLiteral) operands[2],
        (SqlNodeList) operands[3],
        (SqlNodeList) operands[4]);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
      final RelDataTypeFactory typeFactory = validator.getTypeFactory();
      SqlVacuumTable sqlVacuumTable = (SqlVacuumTable) call;
      return getTableOutputRelDataType(typeFactory,
        new VacuumOptions(sqlVacuumTable.expireSnapshots.booleanValue(), sqlVacuumTable.removeOrphans.booleanValue(), null, null, null, null));
    }
  };

  private final SqlIdentifier table;

  /**
   * Creates a SqlVacuum.
   */
  public SqlVacuumTable(
    SqlParserPos pos,
    SqlIdentifier table,
    SqlLiteral expireSnapshots,
    SqlLiteral removeOrphans,
    SqlNodeList optionsList,
    SqlNodeList optionsValueList) {
    super(pos, expireSnapshots, removeOrphans, optionsList, optionsValueList);
    this.table = table;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public SqlIdentifier getTable() {
    return table;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(
        table,
        expireSnapshots,
        removeOrphans,
        optionsList,
        optionsValueList);
  }

  @Override
  public NamespaceKey getPath() {
    return new NamespaceKey(table.names);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("VACUUM");
    writer.keyword("TABLE");
    table.unparse(writer, leftPrec, rightPrec);

    if (expireSnapshots.booleanValue()) {
      writer.keyword("EXPIRE");
      writer.keyword("SNAPSHOTS");
      if(optionsList != null) {
        for (int i = 0; i < optionsList.size(); i++) {
          optionsList.get(i).unparse(writer, leftPrec, rightPrec);
          optionsValueList.get(i).unparse(writer, leftPrec, rightPrec);
        }
      }
    } else if (removeOrphans.booleanValue()) {
      writer.keyword("REMOVE");
      writer.keyword("ORPHAN");
      writer.keyword("FILES");
      if(optionsList != null) {
        for (int i = 0; i < optionsList.size(); i++) {
          optionsList.get(i).unparse(writer, leftPrec, rightPrec);
          optionsValueList.get(i).unparse(writer, leftPrec, rightPrec);
        }
      }
    }
  }

  @Override
  public SqlToPlanHandler toPlanHandler() {
    return new VacuumTableHandler();
  }

  @Override
  public RelNode convertToRel(RelOptCluster cluster, Prepare.CatalogReader catalogReader, RelNode inputRel,
                              RelOptTable.ToRelContext relContext) {
    Prepare.PreparingTable nsTable = catalogReader.getTable(this.getPath().getPathComponents());
    return new VacuumTableCrel(cluster, cluster.traitSetOf(Convention.NONE), nsTable.toRel(relContext), nsTable,
        null, getVacuumOptions());
  }
}
