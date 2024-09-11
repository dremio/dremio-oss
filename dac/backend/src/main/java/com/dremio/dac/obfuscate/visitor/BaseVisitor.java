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
package com.dremio.dac.obfuscate.visitor;

import static com.dremio.dac.obfuscate.visitor.ObfuscationRegistry.processSqlCall;

import com.dremio.dac.obfuscate.ObfuscationUtils;
import com.dremio.exec.planner.sql.parser.SqlDeleteFromTable;
import com.dremio.exec.planner.sql.parser.SqlInsertTable;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.exec.planner.sql.parser.SqlUpdateTable;
import com.dremio.exec.planner.sql.parser.TableVersionSpec;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * This is the core visitor class containing {@code visit()} methods for each SQL command that
 * requires obfuscation in the SQL query. Inside each {@code visit()} function, if the operand to be
 * obfuscated is a table name, alias or a dataset name, pass in an instance of {@link
 * TableNameVisitor} to the {@code accept()} method. Else, if it is a literal, identifier or another
 * nested subquery, pass an instance of {@link BaseVisitor} itself, that is, {@code this} for
 * recursion. Ensure the {@code visit()} function is added inside {@link
 * ObfuscationRegistry#processSqlCall}
 */
public class BaseVisitor extends SqlShuttle {

  private static final TableNameVisitor tableNameVisitor = new TableNameVisitor();

  BaseVisitor() {}

  @Override
  public SqlNode visit(SqlLiteral literal) {
    if (!ObfuscationUtils.shouldObfuscatePartial()) {
      return literal;
    }
    if (literal instanceof SqlCharStringLiteral) {
      SqlCharStringLiteral charStringLiteral = (SqlCharStringLiteral) literal;
      String value = charStringLiteral.toValue();
      String newValue = "literal_" + Integer.toHexString(value.hashCode());
      return SqlLiteral.createCharString(
          newValue, charStringLiteral.getNlsString().getCharsetName(), literal.getParserPosition());
    }
    return literal;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (!ObfuscationUtils.shouldObfuscateFull()) {
      return id;
    }
    List<String> names = new ArrayList<>(id.names);
    for (int i = 0; i < names.size() - 1; i++) {
      names.set(i, Integer.toHexString(names.get(i).toLowerCase().hashCode()));
    }
    names.set(
        names.size() - 1,
        "field_" + Integer.toHexString(names.get(names.size() - 1).toLowerCase().hashCode()));
    return new SqlIdentifier(names, id.getCollation(), id.getParserPosition(), null);
  }

  public SqlNode visit(SqlInsertTable call) {
    List<SqlNode> operands = new ArrayList<>(call.getOperandList());

    // visit table name
    if (call.getTargetTable() != null) {
      operands.set(0, call.getTargetTable().accept(tableNameVisitor));
    }

    // visit table spec version specifier
    if (call.getTableVersionSpec() != null
        && call.getTableVersionSpec().getVersionSpecifier() != null) {
      TableVersionSpec spec = call.getTableVersionSpec();
      SqlTableVersionSpec newSpec =
          new SqlTableVersionSpec(
              call.getParserPosition(),
              spec.getTableVersionType(),
              spec.getVersionSpecifier().accept(this),
              spec.getTimestamp());
      operands.set(3, newSpec);
    }

    // visit column names
    if (call.getTargetColumnList() != null) {
      operands.set(2, call.getTargetColumnList().accept(this));
    }

    // visit values
    if (call.getQuery() != null) {
      operands.set(1, call.getQuery().accept(this));
    }
    return call.getOperator().createCall(call.getParserPosition(), operands);
  }

  public SqlNode visit(SqlSelect call) {
    List<SqlNode> ops = new ArrayList<>(call.getOperandList());
    if (call.getSelectList() != null) {
      ops.set(1, call.getSelectList().accept(this));
    }

    if (call.getFrom() != null) {
      ops.set(2, call.getFrom().accept(tableNameVisitor));
    }

    if (call.hasWhere()) {
      ops.set(3, call.getWhere().accept(this));
    }
    if (call.getGroup() != null) {
      ops.set(4, call.getGroup().accept(this));
    }
    if (call.getHaving() != null) {
      ops.set(5, call.getHaving().accept(this));
    }
    if (call.hasOrderBy()) {
      ops.set(7, call.getOrderList().accept(this));
    }
    return call.getOperator().createCall(call.getParserPosition(), ops);
  }

  public SqlNode visit(SqlJoin call) {
    List<SqlNode> ops = new ArrayList<>(call.getOperandList());
    if (call.getLeft() != null) {
      ops.set(0, call.getLeft().accept(tableNameVisitor));
    }
    if (call.getRight() != null) {
      ops.set(3, call.getRight().accept(tableNameVisitor));
    }
    if (call.getCondition() != null) {
      ops.set(5, call.getCondition().accept(this));
    }
    return call.getOperator().createCall(call.getParserPosition(), ops);
  }

  public SqlNode visit(SqlOrderBy call) {
    List<SqlNode> ops = new ArrayList<>(call.getOperandList());
    if (call.query != null) {
      ops.set(0, call.query.accept(this));
    }
    if (call.orderList != null) {
      ops.set(1, call.orderList.accept(this));
    }
    return call.getOperator().createCall(call.getParserPosition(), ops);
  }

  public SqlNode visit(SqlWithItem call) {
    List<SqlNode> ops = new ArrayList<>(call.getOperandList());
    if (call.name != null) {
      ops.set(1, call.columnList.accept(tableNameVisitor));
    }
    if (call.columnList != null) {
      ops.set(1, call.columnList.accept(this));
    }
    if (call.query != null) {
      ops.set(2, call.query.accept(this));
    }
    return call.getOperator().createCall(call.getParserPosition(), ops);
  }

  public SqlNode visit(SqlWith call) {
    List<SqlNode> ops = new ArrayList<>(call.getOperandList());
    if (call.withList != null) {
      ops.set(0, call.withList.accept(this));
    }
    if (call.body != null) {
      ops.set(1, call.body.accept(this));
    }
    return call.getOperator().createCall(call.getParserPosition(), ops);
  }

  public SqlNode visit(SqlDeleteFromTable call) {
    List<SqlNode> operands = new ArrayList<>(call.getOperandList());

    // visit table name
    if (call.getTargetTable() != null) {
      operands.set(0, call.getTargetTable().accept(tableNameVisitor));
    }

    // visit target table alias
    if (call.getAlias() != null) {
      operands.set(2, call.getAlias().accept(tableNameVisitor));
    }

    // visit the WHERE operand. This could be one or more subqueries separated by AND operator
    if (call.getCondition() != null) {
      operands.set(1, call.getCondition().accept(this));
    }

    return call.getOperator()
        .createCall(call.getParserPosition(), operands.toArray(new SqlNode[0]));
  }

  public SqlNode visit(SqlOptimize call) {
    List<SqlNode> operands = new ArrayList<>(call.getOperandList());

    // visit table name
    if (call.getTable() != null) {
      operands.set(0, call.getTable().accept(tableNameVisitor));
    }

    // visit the partition columns and condition
    if (operands.get(4) != null) {
      operands.set(4, operands.get(4).accept(this));
    }

    return call.getOperator().createCall(call.getParserPosition(), operands);
  }

  public SqlNode visit(SqlUpdateTable call) {
    List<SqlNode> operands = new ArrayList<>(call.getOperandList());

    // visit table name
    if (call.getTargetTable() != null) {
      operands.set(0, call.getTargetTable().accept(tableNameVisitor));
    }

    // visit target column names to be updated
    if (call.getTargetColumnList() != null) {
      operands.set(1, call.getTargetColumnList().accept(this));
    }

    // Visit target column updated values.
    // This could be a list of values, select queries or mix of both
    if (call.getSourceExpressionList() != null) {
      operands.set(2, call.getSourceExpressionList().accept(this));
    }

    // Visit the where condition operands
    if (call.getCondition() != null) {
      operands.set(3, call.getCondition().accept(this));
    }

    // visit the target table alias
    if (call.getAlias() != null) {
      operands.set(4, call.getAlias().accept(tableNameVisitor));
    }

    return call.getOperator().createCall(call.getParserPosition(), operands);
  }

  public SqlNode visit(SqlMerge call) {
    List<SqlNode> operands = new ArrayList<>(call.getOperandList());

    // visit target table
    if (call.getTargetTable() != null) {
      operands.set(0, call.getTargetTable().accept(tableNameVisitor));
    }

    // visit condition
    if (call.getCondition() != null) {
      operands.set(1, call.getCondition().accept(this));
    }

    // visit source table. This could be a table name or a select query returning a view
    if (call.getSourceTableRef() != null) {
      operands.set(2, call.getSourceTableRef().accept(tableNameVisitor));
    }

    // visit the update call operand. This will be an UPDATE query of type SqlUpdateTable.java
    if (call.getUpdateCall() != null) {
      operands.set(3, call.getUpdateCall().accept(this));
    }

    // visit the insert call operand. This will be an INSERT query of type SqlInsertTable.java
    if (call.getInsertCall() != null) {
      operands.set(4, call.getInsertCall().accept(this));
    }

    // visit the target table alias
    if (call.getAlias() != null) {
      operands.set(5, call.getAlias().accept(tableNameVisitor));
    }

    return call.getOperator().createCall(call.getParserPosition(), operands);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    return processSqlCall(call, () -> super.visit(call));
  }
}
